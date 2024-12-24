#!/usr/bin/env python3
import re
import shutil
import sys
from pathlib import Path

import pandas as pd

FOLDER = Path(__file__).parent

#
# Parse file into DataFrame
#
def read_csv(file_name: str):
	lines = [
		line.split(',', 3)
		for line in open(file_name)
	]
	# split the headers
	headers, lines = lines[0], lines[1:]
	# remove \n from last header
	headers[-1] = headers[-1].strip()
	# strip `"data"\n`
	for line in lines:
		line[-1] = line[-1][1:-2]

	df = pd.DataFrame(lines, columns=headers)
	# convert the time (seconds) to int
	df["time"] = pd.to_numeric(df["time"])
	# it seems that the user_name is in lowercase.
	# 	however, usernames in the `message` column are not always lowercase
	# 	thus, we'll always convert to lowercase
	df["user_name"] = df["user_name"].str.lower()
	return df

#
# Simple Statistics
#
def num_messages_per_user(df) -> float:
	num_messages = len(df)
	num_users = len(df.user_name.unique())
	return num_messages / num_users

def stream_time_in_seconds(df) -> int:
	return df.time.max()

#
# Parsing gifts/subs/bits
#
_GIFT_DONATION_REGEX = re.compile(".*? gifted a Tier (\\d) sub to")
def _get_donated_sub(message: str) -> int:
	if (donated_tier := _GIFT_DONATION_REGEX.match(message)):
		return int(donated_tier.group(1))
	else:
		return 0

def add_column_sub_gifts(df: pd.DataFrame) -> pd.DataFrame:
	df["donated_sub"] = df["message"].apply(_get_donated_sub)
	return df

_SUBSCRIBE_PRIME_REGEX = re.compile(".*? subscribed with Prime. They've subscribed for (\\d+) months!")
_SUBSCRIBE_PRIME_WITHOUT_STREAK_REGEX = re.compile(".*? subscribed with Prime.")
_SUBSCRIBE_TIER_REGEX = re.compile(".*? subscribed at Tier (\\d). They've subscribed for (\\d+) months, currently on a (\\d+) month streak!")
_SUBSCRIBE_TIER_WITHOUT_STREAK_REGEX = re.compile(".*? subscribed at Tier (\\d). They've subscribed for (\\d+) months!")
_SUBSCRIBE_TIER_FIRST_SUBSCRIPTION_REGEX = re.compile(".*? subscribed at Tier (\\d).")
def _get_subscription_type(message: str) -> tuple[str, int]:
	if (subscribed := _SUBSCRIBE_PRIME_REGEX.match(message)):
		return "prime", int(subscribed.group(1))
	elif _SUBSCRIBE_PRIME_WITHOUT_STREAK_REGEX.match(message):
		return "prime", 0
	elif (subscribed := _SUBSCRIBE_TIER_REGEX.match(message)) or (subscribed := _SUBSCRIBE_TIER_WITHOUT_STREAK_REGEX.match(message)):
		return f"tier_{subscribed.group(1)}", int(subscribed.group(2))
	elif (subscribed := _SUBSCRIBE_TIER_FIRST_SUBSCRIPTION_REGEX.match(message)):
		return f"tier_{subscribed.group(1)}", 0
	else:
		return "none", 0

def add_column_subscriptions(df: pd.DataFrame) -> pd.DataFrame:
	df["subscription_type"], df["subscription_months"] = zip(*df["message"].apply(_get_subscription_type))
	return df

_BITS_REGEX = re.compile("\\bCheer(\\d+)\\b")
def _get_bits(message: str) -> int:
	if (bits := _BITS_REGEX.search(message)):
		return int(bits.group(1))
	else:
		return 0

def add_column_bits(df: pd.DataFrame) -> pd.DataFrame:
	df["bits"] = df["message"].apply(_get_bits)
	return df

#
# Grouping it all
#
def get_stream_summary(file_path: str) -> dict:
	df = read_csv(file_path)
	df = add_column_sub_gifts(df)
	df = add_column_subscriptions(df)
	df = add_column_bits(df)

	donated_subs = df["donated_sub"].value_counts()
	subscriptions = df["subscription_type"].value_counts()

	stream_time_in_hours = stream_time_in_seconds(df) / 3600

	result = {
		"chat_id": re.search(r"twitch-chat-(\d+)_", file_path.name).group(1),
		"messages_per_user": round(num_messages_per_user(df), 1),
		"stream_time_in_hours": round(stream_time_in_hours, 1),
		"unique_users": len(df.user_name.unique()),
		"total_messages": len(df),

		"donated_tier_1": donated_subs.get(1, 0),
		"donated_tier_2": donated_subs.get(2, 0),
		"donated_tier_3": donated_subs.get(3, 0),

		"subscribed_prime": subscriptions.get("prime", 0),
		"subscribed_tier_1": subscriptions.get("tier_1", 0),
		"subscribed_tier_2": subscriptions.get("tier_2", 0),
		"subscribed_tier_3": subscriptions.get("tier_3", 0),

		"total_bits": df["bits"].sum(),
	}

	result["revenue"] = calculate_revenue(result)

	result["revenue_per_hour"] = result["revenue"] / stream_time_in_hours
	result["revenue_per_user"] = result["revenue"] / result["unique_users"]

	return result

_PRICE_TIER_1 = 5
_PRICE_TIER_2 = 10
_PRICE_TIER_3 = 25
_PRICE_PRIME = _PRICE_TIER_1
_PRICE_BITS = 0.01

def calculate_revenue(result: dict):
	subscriptions_cost = (
		result["donated_tier_1"] * _PRICE_TIER_1 +
		result["donated_tier_2"] * _PRICE_TIER_2 +
		result["donated_tier_3"] * _PRICE_TIER_3 +
		result["subscribed_prime"] * _PRICE_PRIME +
		result["subscribed_tier_1"] * _PRICE_TIER_1 +
		result["subscribed_tier_2"] * _PRICE_TIER_2 +
		result["subscribed_tier_3"] * _PRICE_TIER_3
	)
	return (
		subscriptions_cost / 2 +
		result["total_bits"] * _PRICE_BITS
	)

def create_summary_table_from_folder(folder: str) -> pd.DataFrame:
	files = list(Path(folder).rglob("*.csv"))
	summary = [get_stream_summary(file) for file in files]
	return pd.DataFrame(summary)

def create_summary_table_from_multiple_folders(folders: list[str]) -> pd.DataFrame:
	return pd.concat([create_summary_table_from_folder(folder) for folder in folders])

def calculate_average(df: pd.DataFrame) -> pd.DataFrame:
	averages = []

	for column in df.columns:
		if pd.api.types.is_numeric_dtype(df[column]):
			avg = df[column].mean()
			averages.append(avg)
		else:
			averages.append("")

	averages_row = {col: avg for col, avg in zip(df.columns, averages)}
	avg_df = pd.DataFrame([averages_row])
	return avg_df

if __name__ == '__main__':
	df = create_summary_table_from_multiple_folders(sys.argv[1:] or [FOLDER])
	if df.empty:
		print("You can download stream chat data from `https://www.twitchchatdownloader.com/`")
	else:
		terminal_width = shutil.get_terminal_size((80, 20)).columns
		with pd.option_context('display.width', terminal_width, 'display.max_columns', None):
			print(df)
			print('-' * terminal_width)
			print(calculate_average(df))
