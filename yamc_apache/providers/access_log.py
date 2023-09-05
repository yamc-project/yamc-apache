# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import os
import sys
import pandas as pd
import json

from apache_log_parser import Parser, apachetime, LineDoesntMatchException
import datetime

from yamc.providers import PerformanceProvider, perf_checker, OperationalError
from yamc.component import ValidationError
from yamc.utils import Map


numeric_fields = [
    "time_s",
    "response_bytes",
    "response_bytes_clf",
    "time_us",
    "num_keepalives",
    "bytes_rx",
    "bytes_tx",
]


def round_time_minutes(time, minutes):
    rounded_minutes = (time.minute // minutes) * minutes
    rounded_time = time.replace(minute=rounded_minutes, second=0, microsecond=0)
    return rounded_time


def find_entries(access_log, log_parser, time_from, time_to, chunk_size=1024, parser_errors_threshold=0.2):
    """
    Search for log entries in an access log file based on a specified date and time.
    The date and time are rounded to the nearest minute based on the align_minutes parameter.
    The function returns a list of log entries that match the specified date and time.
    If no entries are found, the function returns None.
    """
    with open(access_log, "rb") as f:
        start = 0
        end = os.path.getsize(access_log)

        # use binary search to find the first log entry that matches the specified date and time
        while True:
            pos = start + (end - start) // 2
            f.seek(pos)
            chunk = f.read(min(chunk_size, end - start)).decode("utf-8")
            lines = chunk.split("\n")

            # the first and last lines may be incomplete, so we need to skip them
            # if there are no lines, we are done and haven't found any log entries
            if len(lines[1:-1]) == 0:
                first_pos = None
                break

            first_pos, second_pos = None, None
            chunk_pos = pos + len(lines[0]) + 1

            for l in lines[1:-1]:
                try:
                    parts = log_parser.parse(l)
                except LineDoesntMatchException as e:
                    continue

                dt = parts["time_received_datetimeobj"]
                if time_from <= dt:
                    first_pos = chunk_pos
                if time_from > dt:
                    second_pos = chunk_pos
                if first_pos is not None and second_pos is not None:
                    break
                chunk_pos += len(l) + 1

            if first_pos is not None and second_pos is None or first_pos is None and second_pos is None:
                end = pos
            elif second_pos is not None and first_pos is None:
                start = pos
            else:
                break

        # if no log entry was found, return None
        if first_pos is None:
            return None

        # read all log entries from the first log entry that matches the specified date and time
        f.seek(first_pos)
        reminder = ""
        done = False
        entries = []
        num_errors = 0
        num_lines = 0

        while not done:
            chunk = f.read(chunk_size).decode("utf-8")
            lines = chunk.split("\n")
            lines[0] = reminder + lines[0]
            has_reminder = chunk[-1] != "\n"
            for l in (
                lines[0:-1] if has_reminder else lines[0:]
            ):  # if last line is not complete, we need to keep it for the next chunk
                if l != "":
                    try:
                        parts = log_parser.parse(l)
                        num_lines += 1
                    except LineDoesntMatchException as e:
                        num_errors += 1
                        continue
                    # dt = _align_time(parts["time_received_datetimeobj"], align_minutes)
                    if parts["time_received_datetimeobj"] < time_to:
                        entries.append(parts)
                    else:
                        done = True
                        break

            reminder = lines[-1] if has_reminder else ""
            done = done or len(chunk) < chunk_size

    if num_lines > 0 and num_errors / num_lines > parser_errors_threshold:
        raise OperationalError(
            f"Too many errors ({(num_errors/num_lines)*100:.2f}%) when parsing log entries. Check the log format."
        )

    return entries


class AccessLogProvider(PerformanceProvider):
    """
    A provider that reads performance data from an Apache access log file.
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.format = self.config.value("format", required=True)
        self.log_parser = Parser(self.format)
        self.access_log = self.config.value("access_log", required=True)
        self.simulated_time = self.config.value("simulated_time.start", default=None)
        self.simulated_time_delta = self.config.value("simulated_time.delta", default=1)
        self.simulated_time_format = self.config.value("simulated_time.format", default="%Y-%m-%d %H:%M:%S")
        self._time = None
        if not "time_received" in self.log_parser.names:
            raise ValidationError("The specified log format does not contain time field (%t).")
        if not "time_s" in self.log_parser.names and not "time_us" in self.log_parser.names:
            raise ValidationError("The specified log format does not contain response time field (%D or %T).")

    @property
    def source(self):
        return self.access_log

    def time(self):
        if self.simulated_time is not None:
            if self._time is None:
                self._time = datetime.datetime.strptime(self.simulated_time, self.simulated_time_format)
            else:
                self._time += datetime.timedelta(minutes=self.simulated_time_delta)
        else:
            self._time = datetime.datetime.now()
        return self._time

    def update(self, id=None, time_delta=1):
        _time = self.time()
        if id is None:
            id = self.component_id
        if self.data is None:
            self.data = Map()
        data = self.data.get(id, Map(data=None, time_from=None, time_to=None))
        if data.data is None or _time > data.time_to:
            data.time_from = round_time_minutes(_time - datetime.timedelta(minutes=time_delta), time_delta)
            data.time_to = round_time_minutes(_time, time_delta)
            entries = find_entries(self.access_log, self.log_parser, data.time_from, data.time_to)

            if entries is not None:
                data.data = pd.DataFrame(entries)
                _fields = [x for x in data.data if x in numeric_fields]
                if len(_fields) > 0:
                    for nf in _fields:
                        data.data[nf] = pd.to_numeric(data.data[nf], errors="coerce")
                    data.data = data.data.dropna(subset=_fields)
                data.updated_time = time.time()

            else:
                data.data = None
                data.updated_time = None
            self.data[id] = data
            return True
        else:
            return False

    @perf_checker(id_arg="id")
    def stats(self, id, time_delta=1, group=None, stats_def=None, filters=None):
        self.update(id=id, time_delta=time_delta)
        if self.data[id].data is None:
            return []
        df = self.data[id].data.copy()

        if filters:
            filter_condition = df.apply(
                lambda row: any(all(row[k] == v for k, v in f.items() if k in row) for f in filters), axis=1
            )
            df = df[filter_condition]

        grouped_df = df.groupby(group)

        result = []
        for group_key, _group in grouped_df:
            stats = {"id": id, "time": self.data[id].time_from.timestamp()}
            for inx, key in enumerate(group):
                stats[key] = group_key[inx]
            for k, f in stats_def.items():
                stats[k] = f(_group)
            result.append(stats)

        return result
