from dynaconf import Dynaconf
from  os.path import join, dirname
import os


root_path = '/home/cip/ce/ix05ogym/Majid/MADE/'

__sp = join(root_path,'settings.toml')
print(__sp)
settings = Dynaconf(settings_files=[__sp] )

settings.paths.data_path = join(root_path, settings.paths.data_path)

__all__ = ['settings']

if __name__ == '__main__':
    print(__sp)
    print(settings.paths.data_path)

    