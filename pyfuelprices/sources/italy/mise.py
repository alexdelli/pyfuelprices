"""Italy MIMIT Open Data CSV source per pyfuelprices.

Come funziona:
  Il MIMIT pubblica ogni mattina alle 8 due file CSV pubblici:
    1. anagrafica_impianti_attivi.csv  ->  posizione e dati di ogni distributore
    2. prezzo_alle_8.csv               ->  prezzi comunicati dai gestori

  Questo modulo scarica entrambi i file, li unisce tramite l'ID impianto,
  e costruisce la lista di FuelLocation compatibile con pyfuelprices.
"""

import logging
import io
import csv
import math
from datetime import timedelta, datetime

from pyfuelprices.const import (
    PROP_FUEL_LOCATION_SOURCE,
    PROP_FUEL_LOCATION_PREVENT_CACHE_CLEANUP,
    PROP_FUEL_LOCATION_SOURCE_ID,
    PROP_AREA_LAT,
    PROP_AREA_LONG,
    PROP_AREA_RADIUS,
)
from pyfuelprices.sources import Source
from pyfuelprices.fuel_locations import Fuel, FuelLocation

from .const import (
    MISE_PRICES_CSV_URL,
    MISE_STATIONS_CSV_URL,
    MISE_CSV_SEPARATOR,
    MISE_HEADERS,
    MISE_FUEL_MAPPING,
)

_LOGGER = logging.getLogger(__name__)

_KM_PER_MILE = 1.60934


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Calcola la distanza in km tra due coordinate geografiche."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class MISESource(Source):
    """Sorgente dati carburanti Italia - MIMIT Open Data CSV.

    Utilizza i file CSV ufficiali pubblicati ogni giorno dal Ministero
    delle Imprese e del Made in Italy (MIMIT).

    Documentazione:
      https://www.mimit.gov.it/it/open-data/elenco-dataset/
      carburanti-prezzi-praticati-e-anagrafica-degli-impianti
    """

    country_code = "IT"
    provider_name = "mise_italy"

    _headers = MISE_HEADERS
    update_interval = timedelta(hours=12)

    location_cache: dict[str, FuelLocation] = {}

    _stations_raw: dict[str, dict] = {}
    _prices_raw: dict[str, list] = {}

    async def _download_csv(self, url: str) -> str | None:
        """Scarica un file CSV dall'URL indicato e restituisce il testo."""
        _LOGGER.debug("MISE Italy: scarico CSV da %s", url)
        async with self._client_session.get(url, headers=self._headers) as resp:
            if resp.ok:
                raw_bytes = await resp.read()
                try:
                    return raw_bytes.decode("latin-1")
                except UnicodeDecodeError:
                    return raw_bytes.decode("utf-8", errors="replace")
            _LOGGER.error(
                "MISE Italy: errore HTTP %s scaricando %s", resp.status, url
            )
            return None

    def _parse_stations_csv(self, text: str) -> dict[str, dict]:
        """Legge il CSV anagrafica e restituisce un dizionario id->dati."""
        stations = {}
        lines = text.splitlines()
        content = "\n".join(lines[1:])
        reader = csv.DictReader(io.StringIO(content), delimiter=MISE_CSV_SEPARATOR)
        for row in reader:
            sid = row.get("idImpianto", "").strip()
            if not sid:
                continue
            try:
                lat = float(row.get("Latitudine", "0").replace(",", "."))
                lon = float(row.get("Longitudine", "0").replace(",", "."))
            except ValueError:
                lat, lon = 0.0, 0.0
            stations[sid] = {
                "id":        sid,
                "gestore":   row.get("Gestore", "").strip(),
                "bandiera":  row.get("Bandiera", "").strip(),
                "nome":      row.get("Nome Impianto", "").strip(),
                "indirizzo": row.get("Indirizzo", "").strip(),
                "comune":    row.get("Comune", "").strip(),
                "provincia": row.get("Provincia", "").strip(),
                "lat":       lat,
                "lon":       lon,
            }
        _LOGGER.debug("MISE Italy: lette %d stazioni dall'anagrafica", len(stations))
        return stations

    def _parse_prices_csv(self, text: str) -> dict[str, list]:
        """Legge il CSV prezzi e restituisce un dizionario id->lista prezzi."""
        prices: dict[str, list] = {}
        lines = text.splitlines()
        content = "\n".join(lines[1:])
        reader = csv.DictReader(io.StringIO(content), delimiter=MISE_CSV_SEPARATOR)
        for row in reader:
            sid = row.get("idImpianto", "").strip()
            if not sid:
                continue
            try:
                price = float(row.get("prezzo", "0").replace(",", "."))
            except ValueError:
                continue
            if price <= 0:
                continue
            prices.setdefault(sid, []).append({
                "descCarburante": row.get("descCarburante", "").strip(),
                "prezzo":         price,
                "isSelf":         row.get("isSelf", "0").strip() == "1",
            })
        _LOGGER.debug("MISE Italy: letti prezzi per %d stazioni", len(prices))
        return prices

    async def update(self, areas=None, force=False) -> list[FuelLocation]:
        """Scarica i due CSV e aggiorna la cache completa."""
        if self.next_update > datetime.now() and not force:
            _LOGGER.debug("MISE Italy: aggiornamento non necessario")
            return list(self.location_cache.values())

        _LOGGER.debug("MISE Italy: avvio aggiornamento dati")

        stations_text = await self._download_csv(MISE_STATIONS_CSV_URL)
        prices_text   = await self._download_csv(MISE_PRICES_CSV_URL)

        if stations_text is None or prices_text is None:
            _LOGGER.error("MISE Italy: impossibile scaricare i CSV, aggiornamento saltato")
            return list(self.location_cache.values())

        self._stations_raw = self._parse_stations_csv(stations_text)
        self._prices_raw   = self._parse_prices_csv(prices_text)

        for sid, station in self._stations_raw.items():
            if sid not in self._prices_raw:
                continue
            if station["lat"] == 0.0 and station["lon"] == 0.0:
                continue

            loc = self._build_location(station, self._prices_raw[sid])
            if loc.id not in self.location_cache:
                self.location_cache[loc.id] = loc
            else:
                await self.location_cache[loc.id].update(loc)

        self.next_update = datetime.now() + self.update_interval
        _LOGGER.debug(
            "MISE Italy: cache aggiornata con %d stazioni", len(self.location_cache)
        )
        return list(self.location_cache.values())

    async def search_sites(self, coordinates, radius: float) -> list[dict]:
        """Restituisce le stazioni nel raggio (in miglia) dalle coordinate date."""
        if not self.location_cache:
            await self.update(force=True)

        radius_km = radius * _KM_PER_MILE
        results = []
        for loc in self.location_cache.values():
            dist_km = _haversine_km(
                coordinates[0], coordinates[1], loc.lat, loc.long
            )
            if dist_km <= radius_km:
                await loc.dynamic_build_fuels()
                results.append({**loc.__dict__, "distance": dist_km / _KM_PER_MILE})
        return results

    async def update_area(self, area: dict) -> bool:
        """Richiesto dalla classe base; deleghiamo tutto a update()."""
        await self.update()
        return True

    async def parse_response(self, response) -> list[FuelLocation]:
        """Non usato in questa implementazione."""
        return list(self.location_cache.values())

    def _build_location(self, station: dict, raw_prices: list) -> FuelLocation:
        """Crea un oggetto FuelLocation dai dati di una stazione."""
        site_id = f"{self.provider_name}_{station['id']}"
        brand   = station["bandiera"] or station["gestore"]
        name    = station["nome"] or brand
        address = ", ".join(filter(None, [
            station["indirizzo"],
            station["comune"],
            station["provincia"],
        ]))

        loc = FuelLocation.create(
            site_id=site_id,
            name=name,
            address=address,
            lat=station["lat"],
            long=station["lon"],
            brand=brand,
            available_fuels=self.parse_fuels(raw_prices),
            postal_code="",
            currency="EUR",
            props={
                PROP_FUEL_LOCATION_SOURCE:                self.provider_name,
                PROP_FUEL_LOCATION_SOURCE_ID:             station["id"],
                PROP_FUEL_LOCATION_PREVENT_CACHE_CLEANUP: True,
            },
        )
        loc.next_update = (
            self.next_update
            if self.next_update > datetime.now()
            else self.next_update + self.update_interval
        )
        return loc

    def parse_fuels(self, raw_prices: list) -> list[Fuel]:
        """Converte la lista prezzi in oggetti Fuel, preferendo il self-service."""
        best: dict[str, float] = {}
        for entry in raw_prices:
            raw_name = entry.get("descCarburante", "")
            fuel_key = MISE_FUEL_MAPPING.get(raw_name, raw_name.upper())
            price    = entry.get("prezzo", 0.0)
            is_self  = entry.get("isSelf", False)
            if fuel_key not in best or is_self:
                best[fuel_key] = price
        return [Fuel(fuel_type=k, cost=v, props={}) for k, v in best.items()]
