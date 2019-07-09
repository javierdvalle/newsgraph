import os
import importlib


def load_instance(instance_dir=None, settings_files=['settings.py'], config=None):
    if instance_dir is None:
        # instance is expected to be placed at the root,
        # at the same level of other packages
        instance_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'instance')

    config = config or {}
    for file in settings_files:
        path = os.path.join(instance_dir, file)
        # print('Loading config file {}'.format(path))
        c = load_config_from_file(path)
        config.update(c)

    if 'SERVICE_ACCOUNT_JSON' in config:
        path = os.path.join(instance_dir, config['SERVICE_ACCOUNT_JSON'])
        config['SERVICE_ACCOUNT_PATH'] = path

    return config


def load_class(class_path, *args, **kwargs):
    module_name, class_name = class_path.split(':')
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    return class_(*args, **kwargs)


def get_config_from_module(settings_module):
    config = {}
    for setting in dir(settings_module):
        # you can write your filter here
        if setting.isupper():
            config[setting] = getattr(settings_module, setting)
    return config


def load_config_from_file(path_to_file):
    spec = importlib.util.spec_from_file_location("settings", path_to_file)
    foo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foo)
    return get_config_from_module(foo)


if __name__ == '__main__':
    config = load_instance('/home/javier/wkf/newsgraph/instances/instance1')
    print(config)
