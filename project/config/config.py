from dynaconf import Dynaconf
from  os.path import join, dirname


root_path = '/home/cip/ce/ix05ogym/Majid/MADE/'

__sp = join(root_path,'settings.toml')
settings = Dynaconf(settings_files=['./settings.toml'] )

__all__ = ['settings']