import sys
import types
from types import SimpleNamespace


class ApiException(Exception):
    def __init__(self, status=None, reason=None):
        super().__init__(reason or "")
        self.status = status
        self.reason = reason or ""


class ConfigException(Exception):
    pass


class Configuration:
    @staticmethod
    def get_default_copy():
        return SimpleNamespace(auth_settings=lambda: {"stub": True}, api_key={})

    @staticmethod
    def set_default(cfg):
        return None


def install():
    kube = types.ModuleType("kubernetes")
    client = types.ModuleType("kubernetes.client")
    config = types.ModuleType("kubernetes.config")
    rest = types.ModuleType("kubernetes.client.rest")

    config.ConfigException = ConfigException
    config.load_incluster_config = lambda: None
    config.load_kube_config = lambda: None

    client.Configuration = Configuration
    client.ApiClient = lambda cfg=None: SimpleNamespace()
    client.CoreV1Api = lambda api_client=None: SimpleNamespace()
    client.AppsV1Api = lambda api_client=None: SimpleNamespace()

    rest.ApiException = ApiException

    kube.client = client
    kube.config = config

    # kubernetes.stream.stream — pod exec passthrough. The stub just calls the supplied func and
    # returns its value; a test that injects a fake core_v1 controls the behavior via its
    # connect_get_namespaced_pod_exec stub.
    stream_mod = types.ModuleType("kubernetes.stream")

    def _stream(func, *args, **kwargs):
        return func(*args, **kwargs)

    stream_mod.stream = _stream

    kube.client = client
    kube.config = config
    kube.stream = stream_mod

    sys.modules["kubernetes"] = kube
    sys.modules["kubernetes.client"] = client
    sys.modules["kubernetes.config"] = config
    sys.modules["kubernetes.client.rest"] = rest
    sys.modules["kubernetes.stream"] = stream_mod
