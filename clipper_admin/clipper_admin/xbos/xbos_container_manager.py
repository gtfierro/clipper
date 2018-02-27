from __future__ import absolute_import, division, print_function
from xbos import get_client
from xbos.util import pretty_print_timedelta
from bw2python import ponames
from bw2python.bwtypes import PayloadObject
import threading
import re
import msgpack
import tempfile
import tarfile
import six
import time
from datetime import timedelta
import docker
import logging
import os
import random
from ..container_manager import (
    create_model_container_label, parse_model_container_label,
    ContainerManager, CLIPPER_DOCKER_LABEL, CLIPPER_MODEL_CONTAINER_LABEL,
    CLIPPER_QUERY_FRONTEND_CONTAINER_LABEL,
    CLIPPER_MGMT_FRONTEND_CONTAINER_LABEL, CLIPPER_INTERNAL_RPC_PORT,
    CLIPPER_INTERNAL_QUERY_PORT, CLIPPER_INTERNAL_MANAGEMENT_PORT,
    CLIPPER_INTERNAL_METRIC_PORT)
from ..exceptions import ClipperException
from requests.exceptions import ConnectionError
#from .docker_metric_utils import *

logger = logging.getLogger(__name__)

deploy_regex_str = "[a-z0-9]([-a-z0-9]*[a-z0-9])?\Z"
deployment_regex = re.compile(deploy_regex_str)


def _validate_versioned_model_name(name, version):
    if deployment_regex.match(name) is None:
        raise ClipperException(
            "Invalid value: {name}: a model name must be a valid DNS-1123 "
            " subdomain. It must consist of lower case "
            "alphanumeric characters, '-' or '.', and must start and end with "
            "an alphanumeric character (e.g. 'example.com', regex used for "
            "validation is '{reg}'".format(name=name, reg=deploy_regex_str))
    if deployment_regex.match(version) is None:
        raise ClipperException(
            "Invalid value: {version}: a model version must be a valid DNS-1123 "
            " subdomain. It must consist of lower case "
            "alphanumeric characters, '-' or '.', and must start and end with "
            "an alphanumeric character (e.g. 'example.com', regex used for "
            "validation is '{reg}'".format(
                version=version, reg=deploy_regex_str))


class XBOSContainerManager(ContainerManager):
    def __init__(self,
                 uri="scratch.ns",
                 entity=None, # $BW2_DEFAULT_ENTITY
                 agent="127.0.0.1:28589",
                 extra_container_kwargs={}):
        """
        Parameters
        ----------
        uri : str, optional
            The base BOSSWAVE URI of the model service
        entity : str, optional
            The BOSSWAVE entity to use for the client. Defaults to $BW2_DEFAULT_ENTITY
        agent : str, optional
            The BOSSWAVE agent to connect to. Defaults to 127.0.0.1:28589
        extra_container_kwargs : dict
            Any additional keyword arguments to pass to the call to
            :py:meth:`docker.client.containers.run`.
        """

        # connect BOSSWAVE client to agent + configure entity.
        self.uri = uri.strip('/')
        self.request_uri = "{0}/s.modelserver/_/i.modelserver/slot/request".format(self.uri)
        self.response_uri = "{0}/s.modelserver/_/i.modelserver/signal/response".format(self.uri)
        self.entity = entity
        self.client = get_client(entity=entity) if entity else get_client()
        #self.vk = self.client.setEntity(self.entity)
        self.client.overrideAutoChainTo(True)

        self.check_liveness()

        # manage other container args
        self.extra_container_kwargs = extra_container_kwargs.copy()
        # Merge Clipper-specific labels with any user-provided labels
        if "labels" in self.extra_container_kwargs:
            self.common_labels = self.extra_container_kwargs.pop("labels")
            self.common_labels.update({CLIPPER_DOCKER_LABEL: ""})
        else:
            self.common_labels = {CLIPPER_DOCKER_LABEL: ""}

        container_args = {
            #"network": self.docker_network,
            "detach": True,
        }

        self.extra_container_kwargs.update(container_args)

    def check_liveness(self, timeout=30):
        # check liveness of model service
        responses = self.client.query("{0}/*/s.modelserver/!meta/lastalive".format(self.uri))
        for resp in responses:
            # get the metadata records from the response
            md_records = filter(lambda po: po.type_dotted == ponames.PODFSMetadata, resp.payload_objects)
            # get timestamp field from the first metadata record
            last_seen_timestamp = msgpack.unpackb(md_records[0].content).get('ts')
            # get how long ago that was
            now = time.time()*1e9 # nanoseconds
            # convert to microseconds and get the timedelta
            diff = timedelta(microseconds = (now - last_seen_timestamp)/1e3)
            logger.info("Connected to model server {0} ({1} ago)".format(self.uri, pretty_print_timedelta(diff)))
            if diff.total_seconds() > timeout:
                raise ConnectionError("Model server at {0} is too old".format(self.uri))
    
    def request(self, msg, ponum, timeout=30):
        ev = threading.Event()
        response = []
        def _handleresult(recv):
            got_response=False
            for po in recv.payload_objects:
                data = msgpack.unpackb(po.content)
                allowd = [(2,2,0,1),(2,2,0,3),(2,2,0,5),(2,2,0,7),(2,2,0,9),(2,2,0,11),(2,2,0,13),(2,2,0,15),(2,2,0,17),(2,2,0,19),(2,2,0,21),(2,2,0,23)]
                if po.type_dotted in allowd and data["MsgID"] == msg["MsgID"]:
                    print("Got response")
                    got_response=True
                    response.append(data)
                    break
            if got_response:
                ev.set()
        print("subscribe to","{0}/s.modelserver/_/i.modelserver/signal/response".format(self.uri))
        h = self.client.subscribe("{0}/s.modelserver/_/i.modelserver/signal/response".format(self.uri), _handleresult)
        po = PayloadObject(ponum, None, msgpack.packb(msg))
        print('publish on',"{0}/s.modelserver/_/i.modelserver/slot/request".format(self.uri))
        self.client.publish("{0}/s.modelserver/_/i.modelserver/slot/request".format(self.uri), payload_objects=(po,))
        ev.wait(timeout)
        self.client.unsubscribe(h)
        return response

    def start_clipper(self, query_frontend_image, mgmt_frontend_image,
                      cache_size):
        self.check_liveness()


    def connect(self):
        # No extra connection steps to take on connection
        self.check_liveness()
        return

    def deploy_model(self, name, version, input_type, image, num_replicas=1):
        # Parameters
        # ----------
        # image : str
        #     The fully specified Docker imagesitory to deploy. If using a custom
        #     registry, the registry name must be prepended to the image. For example,
        #     "localhost:5000/my_model_name:my_model_version" or
        #     "quay.io/my_namespace/my_model_name:my_model_version"
        self.set_num_replicas(name, version, input_type, image, num_replicas)

    def register_application(self, name, input_type, default_output, slo_micros):
        self.check_liveness()
        req = {
            "Name": name,
            "Input_type": input_type,
            "Default_output": default_output,
            "Latency_slo_micros": int(slo_micros),
        }
        # send to {get_admin_addr()/admin/adD_app
        req["MsgID"] = int(random.randint(0, 2**32))
        response = self.request(req, (2,2,0,4))
        if len(response) > 0 and response[0].get('Error'):
            raise Exception(response[0].get('Error'))
        pass

    def link_model_to_app(self, app_name, model_name):
        self.check_liveness()
        req = {
            "App_name": app_name,
            "Model_names": [model_name],
        }
        # send to {get_admin_addr()/admin/adD_app
        req["MsgID"] = int(random.randint(0, 2**32))
        response = self.request(req, (2,2,0,6))
        if len(response) > 0 and response[0].get('Error'):
            raise Exception(response[0].get('Error'))

    def build_and_deploy_model(self,
                               name,
                               version,
                               input_type,
                               model_data_path,
                               base_image,
                               labels=None,
                               container_registry=None,
                               num_replicas=1,
                               batch_size=-1):
        self.check_liveness()
        image = self.build_model(name, version, model_data_path, base_image,
                                 container_registry)
        self.deploy_model(name, version, input_type, image, labels,
                          num_replicas, batch_size)
    def build_model(self,
                    name,
                    version,
                    model_data_path,
                    base_image,
                    container_registry=None):
        version = str(version)

        _validate_versioned_model_name(name, version)

        with tempfile.NamedTemporaryFile(
                mode="w+b", suffix="tar") as context_file:
            # Create build context tarfile
            with tarfile.TarFile(
                    fileobj=context_file, mode="w") as context_tar:
                context_tar.add(model_data_path)
                # From https://stackoverflow.com/a/740854/814642
                df_contents = six.StringIO(
                    "FROM {container_name}\nCOPY {data_path} /model/\n".format(
                        container_name=base_image, data_path=model_data_path))
                df_tarinfo = tarfile.TarInfo('Dockerfile')
                df_contents.seek(0, os.SEEK_END)
                df_tarinfo.size = df_contents.tell()
                df_contents.seek(0)
                context_tar.addfile(df_tarinfo, df_contents)
            # Exit Tarfile context manager to finish the tar file
            # Seek back to beginning of file for reading
            context_file.seek(0)
            image = "{name}:{version}".format(name=name, version=version)
            if container_registry is not None:
                image = "{reg}/{image}".format(
                    reg=container_registry, image=image)
            docker_client = docker.from_env()
            logger.info(
                "Building model Docker image with model data from {}".format(
                    model_data_path))
            docker_client.images.build(
                fileobj=context_file, custom_context=True, tag=image)

        logger.info("Pushing model Docker image to {}".format(image))
        docker_client.images.push(repository=image)
        return image

    def deploy_model(self,
                     name,
                     version,
                     input_type,
                     image,
                     labels=None,
                     num_replicas=1,
                     batch_size=-1):
        self.check_liveness()
        version = str(version)
        _validate_versioned_model_name(name, version)
        self.set_num_replicas(
            name=name,
            version=version,
            input_type=input_type,
            image=image,
            num_replicas=num_replicas)
        self.register_model(
            name,
            version,
            input_type,
            image=image,
            labels=labels,
            batch_size=batch_size)
        logger.info("Done deploying model {name}:{version}.".format(
            name=name, version=version))

    def register_model(self,
                       name,
                       version,
                       input_type,
                       image=None,
                       labels=None,
                       batch_size=-1):
        self.check_liveness()
        msgid = random.randint(0, 2**32)
        response = self.request({
            'MsgID': msgid,
            "Model_name": name,
            "Model_version": version,
            "Labels": labels,
            "Input_type": input_type,
            "Container_name": image,
            "Batch_size": batch_size,
        }, (2,2,0,10))
        if len(response) > 0 and response[0].get('Error'):
            raise Exception(response[0].get('Error'))

    def get_current_model_version(self, name):
        self.check_liveness()
        version = None
        model_info = self.get_all_models(verbose=True)
        for m in model_info:
            if m["model_name"] == name and m["is_current_version"]:
                version = m["model_version"]
                break
        if version is None:
            raise ClipperException(
                "No versions of model {} registered with Clipper".format(name))
        return version

    def get_all_apps(self, verbose=False):
        self.check_liveness()
        msgid = random.randint(0, 2**32)
        response = self.request({
            'MsgID': msgid,
            'Verbose': verbose,
        }, (2,2,0,14))
        if len(response) > 0 and response[0].get('Error'):
            raise Exception(response[0].get('Error'))
        elif len(response) > 0 and verbose:
            return response[0]['ApplicationDescriptions']
        elif len(response) > 0 and not verbose:
            return response[0]['ApplicationNames']


    def get_all_models(self, verbose=False):
        self.check_liveness()
        msgid = random.randint(0, 2**32)
        response = self.request({
            'MsgID': msgid,
            'Verbose': verbose,
        }, (2,2,0,12))
        if len(response) > 0 and response[0].get('Error'):
            raise Exception(response[0].get('Error'))
        elif len(response) > 0 and verbose:
            return response[0]['ModelDescriptions']
        elif len(response) > 0 and not verbose:
            return response[0]['ModelNames']

    def get_app_info(self, name):
        self.check_liveness()
        msgid = random.randint(0, 2**32)
        response = self.request({
            'MsgID': msgid,
            'Name': name,
        }, (2,2,0,16))
        if len(response) > 0 and response[0].get('Error'):
            raise Exception(response[0].get('Error'))
        elif len(response) > 0 and response[0].get('Info'):
            return response[0]['Info']
        pass

    def get_model_info(self, name, version):
        self.check_liveness()
        msgid = random.randint(0, 2**32)
        response = self.request({
            'MsgID': msgid,
            'Model_name': name,
            'Model_version': version,
        }, (2,2,0,18))
        if len(response) > 0 and response[0].get('Error'):
            raise Exception(response[0].get('Error'))
        elif len(response) > 0 and response[0].get('Info'):
            return response[0]['Info']

    def get_linked_models(self, app_name):
        self.check_liveness()
        msgid = random.randint(0, 2**32)
        response = self.request({
            'MsgID': msgid,
            'App_name': app_name,
        }, (2,2,0,20))
        if len(response) > 0 and response[0].get('Error'):
            raise Exception(response[0].get('Error'))
        elif len(response) > 0 and response[0].get('Models'):
            return response[0]['Models']

    def get_all_model_replicas(self, verbose=False):
        self.check_liveness()
        msgid = random.randint(0, 2**32)
        response = self.request({
            'MsgID': msgid,
            'Verbose': verbose,
        }, (2,2,0,22))
        if len(response) > 0 and response[0].get('Error'):
            raise Exception(response[0].get('Error'))
        #elif len(response) > 0 and response[0].get('Models'):
        #    return response[0]['Models']

    def get_model_replica_info(self, name, version, replica_id):
        pass
    def get_clipper_logs(self, logging_dir="clipper_logs/"):
        pass

    def inspect_instance(self):
        pass
    def set_model_version(self, name, version, num_replicas=None):
        pass
    def stop_versioned_models(self, model_versions_dict):
        pass
    def stop_inactive_model_versions(self, model_names):
        pass

    def _get_replicas(self, name, version):
        return self._get_with_label("{0}={1}".format(CLIPPER_MODEL_CONTAINER_LABEL, create_model_container_label(name, version)))

    def _get_with_label(self, label):
        msgid = random.randint(0, 2**32)
        response = self.request({
            'MsgID': msgid,
            'Label': label,
        }, (2,2,0,0))
        if len(response) > 0:
            return response[0]["Containers"]
        return []

    def get_num_replicas(self, name, version):
        return len(self._get_replicas(name, version))

    def _add_replica(self, name, version, input_type, image):
        containers = self._get_with_label(CLIPPER_QUERY_FRONTEND_CONTAINER_LABEL)
        if len(containers) < 1:
            logger.warning("No Clipper query frontend found.")
            raise ClipperException(
                "No Clipper query frontend to attach model container to")
        query_frontend_hostname = containers[0]['Names'][0]
        env_vars = {
            "CLIPPER_MODEL_NAME": name,
            "CLIPPER_MODEL_VERSION": version,
            # NOTE: assumes this container being launched on same machine
            # in same docker network as the query frontend
            "CLIPPER_IP": query_frontend_hostname,
            "CLIPPER_INPUT_TYPE": input_type,
        }

        model_container_label = create_model_container_label(name, version)
        labels = self.common_labels.copy()
        labels[CLIPPER_MODEL_CONTAINER_LABEL] = model_container_label

        model_container_name = model_container_label + '-{}'.format(
            random.randint(0, 100000))
        print(image, model_container_name, env_vars, labels, self.extra_container_kwargs)

        msgid = random.randint(0, 2**32)
        response = self.request({
            'MsgID': msgid,
            'Name': name,
            'Version': version,
            'Input_type': input_type,
            'Image': image,
        }, (2,2,0,2))
        print(response)

        ## Metric Section
        #add_to_metric_config(model_container_name,
        #                     CLIPPER_INTERNAL_METRIC_PORT)
        pass


    def set_num_replicas(self, name, version, input_type, image, num_replicas):
        current_replicas = self._get_replicas(name, version)
        if len(current_replicas) < num_replicas:
            num_missing = num_replicas - len(current_replicas)
            logger.info(
                "Found {cur} replicas for {name}:{version}. Adding {missing}".
                format(
                    cur=len(current_replicas),
                    name=name,
                    version=version,
                    missing=(num_missing)))
            for _ in range(num_missing):
                self._add_replica(name, version, input_type, image)
        elif len(current_replicas) > num_replicas:
            num_extra = len(current_replicas) - num_replicas
            logger.info(
                "Found {cur} replicas for {name}:{version}. Removing {extra}".
                format(
                    cur=len(current_replicas),
                    name=name,
                    version=version,
                    extra=(num_extra)))
            while len(current_replicas) > num_replicas:
                cur_container = current_replicas.pop()
                cur_container.stop()
                # Metric Section
                delete_from_metric_config(cur_container.name)

    def get_logs(self, logging_dir):
        containers = self.docker_client.containers.list(
            filters={
                "label": CLIPPER_DOCKER_LABEL
            })
        logging_dir = os.path.abspath(os.path.expanduser(logging_dir))

        log_files = []
        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir)
            logger.info("Created logging directory: %s" % logging_dir)
        for c in containers:
            log_file_name = "image_{image}:container_{id}.log".format(
                image=c.image.short_id, id=c.short_id)
            log_file = os.path.join(logging_dir, log_file_name)
            with open(log_file, "w") as lf:
                lf.write(c.logs(stdout=True, stderr=True))
            log_files.append(log_file)
        return log_files

    def stop_models(self, models):
        containers = self.docker_client.containers.list(
            filters={
                "label": CLIPPER_MODEL_CONTAINER_LABEL
            })
        for c in containers:
            c_name, c_version = parse_model_container_label(
                c.labels[CLIPPER_MODEL_CONTAINER_LABEL])
            if c_name in models and c_version in models[c_name]:
                c.stop()

    def stop_all_model_containers(self):
        containers = self.docker_client.containers.list(
            filters={
                "label": CLIPPER_MODEL_CONTAINER_LABEL
            })
        for c in containers:
            c.stop()

    def stop_all(self):
        containers = self.docker_client.containers.list(
            filters={
                "label": CLIPPER_DOCKER_LABEL
            })
        for c in containers:
            c.stop()

    def get_admin_addr(self):
        return "{host}:{port}".format(
            host=self.public_hostname, port=self.clipper_management_port)

    def get_query_addr(self):
        return None
        #return "{host}:{port}".format(
        #    host=self.public_hostname, port=self.clipper_query_port)
