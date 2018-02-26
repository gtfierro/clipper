from __future__ import absolute_import

from .docker.docker_container_manager import DockerContainerManager
from .kubernetes.kubernetes_container_manager import KubernetesContainerManager
from .xbos.xbos_container_manager import XBOSContainerManager
from .clipper_admin import *
from . import deployers
from .version import __version__
from .exceptions import ClipperException, UnconnectedException
