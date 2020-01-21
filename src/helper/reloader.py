import sys


def _is_module_deletable(modname, modpath):
    if modname.startswith('_cython_inline'):
        # Don't return cached inline compiled .PYX files
        return False
    for path in [sys.prefix]:
        if modpath.startswith(path):
            return False
    else:
        return set(modname.split('.'))


def clear():
    """
    Del user modules to force Python to deeply reload them

    Do not del modules which are considered as system modules, i.e.
    modules installed in subdirectories of Python interpreter's binary
    Do not del C modules
    """
    log = []
    for modname, module in list(sys.modules.items()):
        modpath = getattr(module, '__file__', None)

        if modpath is None:
            # *module* is a C module that is statically linked into the
            # interpreter. There is no way to know its path, so we
            # choose to ignore it.
            continue

        if modname == 'reloader':
            # skip this module
            continue

        modules_to_delete = _is_module_deletable(modname, modpath)
        if modules_to_delete:
            log.append(modname)
            del sys.modules[modname]

    print("Reloaded modules:\n\n%s" % ", ".join(log))


if __name__ == '__main__':

    clear()
