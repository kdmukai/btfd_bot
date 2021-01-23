import datetime
import dateutil


def convert_datetime_str(datetime_str):
	return dateutil.parser.parse(datetime_str).replace(tzinfo=None)


def convert_epoch_to_utc(epoch):
    return datetime.datetime.utcfromtimestamp(epoch)