from typing import Type, Sequence, Union

from multiprocessing import Pool

import datetime

import os
import pathlib
import glob
import shutil

import csv
import json

import secedgar as sec
from secedgar.parser import MetaParser
from secedgar.exceptions import EDGARQueryError, NoFilingsError

import random

USER_AGENT = f"RandomUser{random.randint(0, 255)}@upenn.edu"

DATA_DIR = pathlib.Path(__file__).parent.parent / "data"
SAVE_DIR = DATA_DIR / "filings"

COMPANY_CSV = DATA_DIR / "meta" / "largest_mining_companies_by_market_cap_with_filing.csv"
COMPANY_COL = "symbol"


class Fetcher:

    def __init__(self, 
                 save_dir: str, 
                 user_agent: str, 
                 filing_type: Type[sec.FilingType],
                 start_date: datetime.date = None) -> None:
        self.save_dir    = save_dir
        self.user_agent  = user_agent
        self.filing_type = filing_type
        self.parser      = MetaParser()
        self.start_date  = start_date or datetime.date(2005, 12, 31)

    def process(self, ciks: Union[str, Sequence[str]]):
        """Simple wrapper for batched processing of cik(s)"""

        if isinstance(ciks, str):
            ciks = [ciks]

        print(f"[ INFO ] :: {self.filing_type.value} Fetcher processing {len(ciks)} CIKs...")
        results = [self._fetch_single(cik) for cik in ciks]

        n_failed = 0
        for _, status, msg in results:
            n_failed += int(not status)
            print(msg)
        print(f"[ INFO ] :: {len(results) - n_failed} / {len(results)} processed successfully.")

    def _fetch_single(self, cik: str) -> bool:
        """Driver methods to retrieve filings for a single company (cik)"""

        cik = cik.lower().strip()

        dout = os.path.join(self.save_dir, cik, self.filing_type.value)
        if os.path.exists(dout):
            shutil.rmtree(dout)

        try:
            filings = sec.filings(cik, self.filing_type, self.user_agent, self.start_date)
            filings.save(self.save_dir)

            with Pool(os.cpu_count() - 1) as p:
                p.map(self._parse_single, glob.glob(os.path.join(dout, '*.txt')))
            return cik, True, f"[  OK  ] :: Fetched {self.filing_type.value} for {cik}"
        except EDGARQueryError:
            return cik, False, f"[ FAIL ] :: Unknown CIK: {cik}" 
        except NoFilingsError:
            return cik, False, f"[ FAIL ] :: No {self.filing_type.value} found for CIK {cik}"
        except Exception as e:
            return cik, False, (f"[ FAIL ] :: Unknown error when retrieving {self.filing_type.value} for {cik}; "
                                    f"See error message: {e}")

    def _parse_single(self, fpath: str):
        """Parse a single filing"""

        dout = os.path.splitext(fpath)[0]
        self.parser.process(fpath)

        # Move source file to out-dir
        shutil.move(fpath, os.path.join(dout, '__RAW__.htm'))
        shutil.move(os.path.join(dout, '0.metadata.json'), os.path.join(dout, '__META__.json'))

        # Rename according to metadata
        with open(os.path.join(dout, '__META__.json')) as f:
            metadata = json.load(f)

        droot = pathlib.Path(dout).parent
        os.rename(dout, droot / metadata['FILED_AS_OF_DATE'])


if __name__ == '__main__':

    # Get company list
    with open(COMPANY_CSV) as f:
        ciks = [row[COMPANY_COL] for row in csv.DictReader(f)]

    # Fetch filings
    fetcher_10k = Fetcher(SAVE_DIR, USER_AGENT, sec.FilingType.FILING_10K)
    fetcher_20f = Fetcher(SAVE_DIR, USER_AGENT, sec.FilingType.FILING_20F)
    fetcher_40f = Fetcher(SAVE_DIR, USER_AGENT, sec.FilingType.FILING_40F)

    fetcher_10k.process(ciks)
    fetcher_20f.process(ciks)
    fetcher_40f.process(ciks)
