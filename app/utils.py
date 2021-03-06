import configparser
import os.path

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

config_file = os.path.join(BASE_DIR, 'config.ini')


class ReadConfig:
    def __init__(self, file_path=None):
        self.file_path = file_path if file_path else config_file
        
        self.parser = configparser.ConfigParser()
        self.parser.read(self.file_path,encoding='utf-8')

    def get_section(self, section_name):
        if not self.parser.has_section(section_name.upper()):
            raise ValueError('section_name uncorrect')
        options = self.parser.items(section_name.upper())
        return {name:value for name,value in options if value}

    def get_option(self, section, option):
        if self.parser.has_option(section.upper(), option.lower()):
            return self.parser.get(section.upper(), option.lower())
        else:
            return None

def timestamp_to_datetime(time):
    from datetime import datetime
    time = int(str(time)[:-3])
    return datetime.fromtimestamp(time)

class Singleton:
    _instance = None
    def __new__(cls, **kwargs):
        if not cls._instance:
            cls._instance = super(Singleton, cls).__new__(cls, **kwargs)
        return cls._instance


class SingleMeta(type):
    def __new__(meta, clsname, base, dicts):
        cls = super().__new__(meta, clsname, base, dicts)
        dicts['instance'] = None
        return cls
    def __init__(cls, clsname, base, dicts):
        if not dicts['instance']:
            dicts['instance'] = cls.__new__(cls)
        super().__init__(cls, clsname, base, dicts)

class Sin(metaclass=SingleMeta):
    pass

if __name__ == '__main__':
    # c = ReadConfig()
    # value=c.get_section('db')
    # print(value)
    p = Sin()
    s = Sin()
        
    print(p)
    print(s)
    

        