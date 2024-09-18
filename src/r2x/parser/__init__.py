from .plexos import PlexosParser
from .reeds import ReEDSParser

parser_list = {
    "plexos": PlexosParser,
    "reeds-US": ReEDSParser,
}
