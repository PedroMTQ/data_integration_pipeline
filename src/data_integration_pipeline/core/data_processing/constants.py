import re
from string import punctuation


ONLY_DIGIT_AND_PUNCTUATION_PATTERN = re.compile(r"[\d{}\s]+$".format(re.escape(punctuation)))
