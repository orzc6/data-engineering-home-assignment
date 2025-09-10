from src.models.stocks.gold.transformers.average_daily_return_transformer import AverageDailyReturnTransformer
from src.models.stocks.gold.transformers.highest_worth_transformer import HighestWorthTransformer
from src.models.stocks.gold.transformers.most_volatile_transformer import MostVolatileTransformer
from src.models.stocks.gold.transformers.top_30day_returns_transformer import Top30DayReturnsTransformer
from src.common.logging_utils import get_logger

transformers = {
    "average_daily_return": AverageDailyReturnTransformer(logger=get_logger(__name__)),
    "highest_worth": HighestWorthTransformer(logger=get_logger(__name__)),
    "most_volatile": MostVolatileTransformer(logger=get_logger(__name__)),
    "top_30day_returns": Top30DayReturnsTransformer(logger=get_logger(__name__)),
}

