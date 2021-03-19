import re
from typing import Optional, Tuple

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange
from decimal import Decimal

CENTRALIZED = False

EXAMPLE_PAIR = "ETH-USD"

DEFAULT_FEES = [0.050, 0.150]


RE_4_LETTERS_QUOTE = re.compile(r"^(\w+)(USDT|USDC|USDS|TUSD|BUSD|IDRT|BKRW|BIDR)$")
RE_3_LETTERS_QUOTE = re.compile(r"^(\w+)(BTC|ETH|BNB|DAI|XRP|PAX|TRX|NGN|RUB|TRY|EUR|ZAR|UAH|GBP|USD|BRL)$")


# Helper Functions ---
def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
    try:
        m = RE_4_LETTERS_QUOTE.match(trading_pair)
        if m is None:
            m = RE_3_LETTERS_QUOTE.match(trading_pair)
        return m.group(1), m.group(2)
    except Exception as e:
        raise e

def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    if split_trading_pair(exchange_trading_pair) is None:
        return None
    base_asset, quote_asset = split_trading_pair(exchange_trading_pair)
    return f"{base_asset}-{quote_asset}"

def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    return hb_trading_pair.replace("-", "")

KEYS = {
    "dydx_perpetual_api_key":
        ConfigVar(key="dydx_perpetual_api_key",
                  prompt="Enter your dydx Perpetual API key >>> ",
                  required_if=using_exchange("dydx_perpetual"),
                  is_secure=True,
                  is_connect_key=True),
    "dydx_perpetual_api_secret":
        ConfigVar(key="dydx_perpetual_api_secret",
                  prompt="Enter your dydx Perpetual API secret >>> ",
                  required_if=using_exchange("dydx_perpetual"),
                  is_secure=True,
                  is_connect_key=True),
    "dydx_perpetual_api_passphrase":
        ConfigVar(key="dydx_perpetual_api_passphrase",
                  prompt="Enter your dydx Perpetual API passphrase >>> ",
                  required_if=using_exchange("dydx_perpetual"),
                  is_secure=True,
                  is_connect_key=True),
    "dydx_perpetual_eth_wallet":
        ConfigVar(key="dydx_perpetual_eth_wallet",
                  prompt="Enter your dydx Perpetual ETH wallet address >>> ",
                  required_if=using_exchange("dydx_perpetual"),
                  is_secure=True,
                  is_connect_key=True),
}

OTHER_DOMAINS = ["dydx_perpetual_testnet"]
OTHER_DOMAINS_PARAMETER = {"dydx_perpetual_testnet": "dydx_perpetual_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"dydx_perpetual_testnet": "ETH-USD"}
OTHER_DOMAINS_DEFAULT_FEES = {"dydx_perpetual_testnet": [0.050, 0.150]}
OTHER_DOMAINS_KEYS = {"dydx_perpetual_testnet": {
    # add keys for testnet
    "dydx_perpetual_testnet_api_key":
        ConfigVar(key="dydx_perpetual_testnet_api_key",
                  prompt="Enter your dydx Perpetual testnet API key >>> ",
                  required_if=using_exchange("dydx_perpetual_testnet"),
                  is_secure=True,
                  is_connect_key=True),
    "dydx_perpetual_testnet_api_secret":
        ConfigVar(key="dydx_perpetual_testnet_api_secret",
                  prompt="Enter your dydx Perpetual testnet API secret >>> ",
                  required_if=using_exchange("dydx_perpetual_testnet"),
                  is_secure=True,
                  is_connect_key=True),
    "dydx_perpetual_testnet_api_passphrase":
        ConfigVar(key="dydx_perpetual_testnet_api_passphrase",
                  prompt="Enter your dydx Perpetual testnet API passphrase >>> ",
                  required_if=using_exchange("dydx_perpetual"),
                  is_secure=True,
                  is_connect_key=True),
    "dydx_perpetual_testnet_eth_wallet":
        ConfigVar(key="dydx_perpetual_testnet_eth_wallet",
                  prompt="Enter your dydx Perpetual testnet ETH wallet address >>> ",
                  required_if=using_exchange("dydx_perpetual"),
                  is_secure=True,
                  is_connect_key=True),
}}

DYDX_FEE_SCHEDULE = {
    'level1': {
        'maker': Decimal(0.00050),
        'taker': Decimal(0.00150),
    },
    'level2': {
        'maker': Decimal(0.00025),
        'taker': Decimal(0.00125),
    },
    'level3': {
        'maker': Decimal(0.00010),
        'taker': Decimal(0.00100),
    },
    'level4': {
        'maker': Decimal(0),
        'taker': Decimal(0.00075),
    },
    'level5': {
        'maker': Decimal(0),
        'taker': Decimal(0.00060),
    },
    'VIP': {
        'maker': Decimal(0),
        'taker': Decimal(0.00050),
    }
}        