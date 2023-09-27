import os
from dotenv import load_dotenv
from typing import Union, get_type_hints

load_dotenv()

class AppConfigError(Exception):
    pass

def _parse_bool(val: Union[str, bool]) -> bool:
    return val if type(val) == bool else val.lower() in ['true', 'yes', '1']


class AppConfig:
    DEBUG: bool = False
    ENV: str = 'development'
    DB: str
    USER: str
    PASSWORD: str
    HOST: str
    PORT: int
    DATABASE_URL: str
    
    def __init__(self, env):
        for field in self.__annotations__:
            if not field.isupper():
                continue


            # Raise AppConfig Error if required field not supplied
            default_value = getattr(self, field, None)
            if default_value is None and env.get(field) is None:
                raise AppConfigError('The {} field is required'.format(field))

            # Cast env var value to expect type and raise AppConfigError
            try:
                var_type = get_type_hints(AppConfig)[field]
                if var_type == bool:
                    value = _parse_bool(env.get(field, default_value))
                else:
                    value = var_type(env.get(field, default_value))

                self.__setattr__(field, value)
            except ValueError:
                raise AppConfigError('Unable to cast value of "{}" to type "{}" for "{}" field'.format(
                    env[field],
                    var_type,
                    field
                )
            )

    def __repr__(self):
        return str(self.__dict__)

# Expose Config object for app to import
Config = AppConfig(os.environ)