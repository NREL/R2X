from .plexos import PlexosParser
from .reeds import ReEDSParser

#TODO remove parser_list and imports
parser_list = {
    "plexos": PlexosParser,
    "reeds-US": ReEDSParser,
}
