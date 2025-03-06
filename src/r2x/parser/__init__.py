from .plexos import PlexosParser
from .reeds import ReEDSParser

parser_list = {
    "plexos": PlexosParser,
    "reeds-US": ReEDSParser,
}


external_parsers={}

def register_external_parser(name, parser_class):
    """Register an external parser.

    Parameters
    ----------
    name : str
        The name of the parser to register
    parser_class : type
        The parser class to register
    """
    external_parsers[name] = parser_class
    parser_list[name] = parser_class
