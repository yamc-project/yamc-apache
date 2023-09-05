# -*- coding: utf-8 -*-
# @author: Tomas Vitvar, https://vitvar.com, tomas@vitvar.com

import time
import json
from datetime import datetime

from yamc.providers import HttpProvider, PerformanceProvider, perf_checker, OperationalError

# scoreboard meanings
scoreboard_meanings = {
    "_": "waiting_connection",
    "S": "starting_up",
    "R": "reading_request",
    "W": "sending_reply",
    "K": "keepalive_read",
    "D": "dns_lookup",
    "C": "closing_connection",
    "L": "logging",
    "G": "gracefully_finishing",
    "I": "idle_cleanup",
    ".": "open_slot",
}


class ModStatusProvider(HttpProvider, PerformanceProvider):
    """
    ModStatusProvider is a provider that reads the mod_status page of the Apache server
    """

    def __init__(self, config, component_id):
        super().__init__(config, component_id)
        self.date_format = self.config.value("date_format", default="%A, %d-%b-%Y %H:%M:%S %Z")
        self.mod_status = None

    def parse_auto_content(self, content):
        """
        Parses the output of the Apache mod_status page.
        """

        # conversion functions for mod_status data
        conversion = {
            "CurrentTime": lambda x: datetime.strptime(x, self.date_format),
            "RestartTime": lambda x: datetime.strptime(x, self.date_format),
            "BusyWorkers": int,
            "BytesPerReq": float,
            "BytesPerSec": float,
            "CPUChildrenSystem": float,
            "CPUChildrenUser": float,
            "CPULoad": float,
            "CPUSystem": float,
            "CPUUser": float,
            "CacheCurrentEntries": int,
            "CacheDiscardCount": int,
            "CacheExpireCount": int,
            "CacheIndexUsage": str,
            "CacheIndexesPerSubcaches": int,
            "CacheRemoveHitCount": int,
            "CacheRemoveMissCount": int,
            "CacheReplaceCount": int,
            "CacheRetrieveHitCount": int,
            "CacheRetrieveMissCount": int,
            "CacheSharedMemory": int,
            "CacheStoreCount": int,
            "CacheSubcaches": int,
            "CacheType": str,
            "CacheUsage": str,
            "ConnsAsyncClosing": int,
            "ConnsAsyncKeepAlive": int,
            "ConnsAsyncWriting": int,
            "ConnsTotal": int,
            "DurationPerReq": float,
            "IdleWorkers": int,
            "Load1": float,
            "Load15": float,
            "Load5": float,
            "ParentServerConfigGeneration": int,
            "ParentServerMPMGeneration": int,
            "Processes": int,
            "ReqPerSec": float,
            "ServerMPM": str,
            "ServerUptime": str,
            "ServerUptimeSeconds": int,
            "ServerVersion": str,
            "Server_Built": str,
            "Stopping": int,
            "Total_Accesses": int,
            "Total_Duration": int,
            "Total_kBytes": int,
            "Uptime": int,
            "sb_open_slot": int,
            "sb_sending_reply": int,
            "sb_waiting_connection": int,
            "sb_waiting_connection": int,
            "sb_starting_up": int,
            "sb_reading_request": int,
            "sb_sending_reply": int,
            "sb_keepalive_read": int,
            "sb_dns_lookup": int,
            "sb_closing_connection": int,
            "sb_logging": int,
            "sb_gracefully_finishing": int,
            "sb_idle_cleanup": int,
            "sb_open_slot": int,
        }

        def _default_conversion(x):
            try:
                return float(x)
            except ValueError:
                return str(x)

        result = {}

        lines = content.strip().split("\n")
        for line in lines:
            if ":" in line:
                key, value = line.split(":", 1)
                key = key.strip().replace(" ", "_")
                value = conversion.get(key, _default_conversion)(value.strip())
                if key == "Scoreboard":
                    scoreboard = {}
                    for char in value:
                        scoreboard[scoreboard_meanings.get(char, "unknown")] = (
                            scoreboard.get(scoreboard_meanings.get(char, "unknown"), 0) + 1
                        )
                    for k, v in scoreboard.items():
                        result["sb_" + k] = v
                else:
                    result[key] = value

        if not result.get("ServerVersion"):
            raise Exception("Cannot parse the Apache mod_status. The properry ServerVersion cannot be found.")

        self.log.debug(
            f'Apache mod_status parsed. ServerVersion={result["ServerVersion"]}, Server_Built={result["Server_Built"]}, '
            + f'ServerUptime={result["ServerUptime"]}'
        )
        return result

    @perf_checker(id_arg="name")
    def get(self, name):
        """
        Returns the data from the mod_status page of the Apache server. The attribute `name` is added to the result
        to identify the server.
        """
        try:
            if self.update():
                self.mod_status = self.parse_auto_content(self.data.decode("utf-8"))
                self.mod_status["name"] = name
            return self.mod_status
        except Exception as e:
            raise OperationalError("Cannot get data from Apache mod_status page: %s" % e, e)
