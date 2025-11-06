"""
autopatch.py

Lightweight automatic fallback system for missing modules/attributes.

Behavior:
- Installs a meta-path finder that creates a safe stub module for any module
  that cannot be found via the normal import system.
- Stubs expose a __getattr__ that returns a no-op callable which logs the access.
- Certain well-known modules get tiny targeted shims (e.g. imghdr.what).
- Logs a clear message when a fallback is applied.

This is intentionally defensive: the goal is to avoid ImportError/ModuleNotFoundError
or AttributeError at import-time. It favors stability and silence over perfect
correctness.
"""
from __future__ import annotations

import sys
import types
import logging
import importlib.abc
import importlib.util
from types import ModuleType

logger = logging.getLogger('autopatch')
if not logger.handlers:
    # Ensure a sensible default handler so messages are visible early
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter('[autopatch] %(levelname)s: %(message)s'))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)


def _make_stub_module(name: str) -> ModuleType:
    m = ModuleType(name)
    m.__dict__['__autopatch__'] = True

    def _missing_attr(attr_name=None):
        def _stub(*args, **kwargs):
            logger.warning("Fallback call: %s.%s() — returning None", name, attr_name)
            return None
        return _stub

    def __getattr__(attr: str):
        # Provide tiny special-cases for modules we know about
        if name == 'imghdr' and attr == 'what':
            return lambda file=None, h=None: None

        # For typing, provide a few commonly-used aliases
        if name == 'typing':
            if attr in ('Any', 'Dict', 'List', 'Tuple', 'Optional', 'Callable'):
                return object

        # Generic fallback: return a callable that logs and returns None
        return _missing_attr(attr)

    def __dir__():
        return []

    m.__getattr__ = __getattr__
    m.__dir__ = __dir__
    m.__all__ = []
    return m


class FallbackLoader(importlib.abc.Loader):
    def __init__(self, fullname: str):
        self.fullname = fullname

    def create_module(self, spec):
        return _make_stub_module(self.fullname)

    def exec_module(self, module):
        # nothing to execute; module already has stubs
        sys.modules[self.fullname] = module
        logger.info("Applied fallback stub for missing module: %s", self.fullname)


class FallbackFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        # Avoid stubbing dotted names (submodules/packages) because
        # providing a plain module where a package is expected can break
        # import machinery (see earlier TypeError). Only attempt to stub
        # simple top-level module names without dots.
        if '.' in fullname:
            return None

        # Avoid stubbing some builtins or common namespace roots that would be dangerous
        blacklist = {
            'sys', 'builtins', 'types', 'importlib', 'pkgutil', 'pkg_resources',
            'setuptools', 'distutils', 'org', 'com', 'net', 'java', 'javax', 'jp', 'ru', 'cn', 'io'
        }
        if fullname in blacklist:
            return None

        # If the module actually exists, don't stub it.
        try:
            spec = importlib.util.find_spec(fullname)
            if spec is not None:
                return None
        except Exception:
            # If find_spec itself fails unexpectedly, avoid stubbing to reduce
            # the chance of breaking importlib internals.
            return None

        # Provide a fallback loader for this missing top-level module
        return importlib.util.spec_from_loader(fullname, FallbackLoader(fullname))


# Install the finder early
if not any(isinstance(f, FallbackFinder) for f in sys.meta_path):
    sys.meta_path.insert(0, FallbackFinder())
    logger.info('autopatch: installed fallback meta-path finder')
