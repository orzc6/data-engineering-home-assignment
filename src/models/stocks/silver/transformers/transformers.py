from src.models.stocks.silver.transformers.data_cleaning_transformer import DataCleaningTransformer
from src.models.stocks.silver.transformers.daily_returns_transformer import DailyReturnsTransformer
from src.models.stocks.silver.transformers.worth_calculation_transformer import WorthCalculationTransformer
from src.common.logging_utils import get_logger

transformers = {
    "data_cleaning": DataCleaningTransformer(logger=get_logger(__name__)),
    "daily_returns": DailyReturnsTransformer(logger=get_logger(__name__)),
    "worth_calculation": WorthCalculationTransformer(logger=get_logger(__name__)),
}