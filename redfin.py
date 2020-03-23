from db import get_counties
import json
import os
import pandas as pd
import pdb
from progressbar import progressbar
import requests
import requests_cache
from tenacity import retry, stop_after_attempt


def check_redfin_counties():
    requests_cache.install_cache("redfin")

    def get_county_urls(counties):
        @retry(stop=stop_after_attempt(5))
        def request_with_retry(url, params):
            resp = requests.get(url, params=params)
            if resp.status_code != 200:
                status_code = resp.status_code
                print(resp.status_code, "status code")
                raise ValueError(f"{status_code} status code error")
            else:
                return resp

        def replace_strs(input_county):
            str_replacements = [
                ("saint", "st.")
            ]
            for key, value in str_replacements:
                input_county = input_county.replace(key, value)
            return input_county

        # https://www.redfin.com/stingray/do/location-autocomplete?location=coconino%20county%2C%20az&start=0&count=10&v=2&market=virginia&al=1&iss=false&ooa=true&mrs=false  # noqa:E501
        checked_counties = []
        problem_counties = []
        for county_row in progressbar(counties):
            try:
                county = county_row["county"].lower().replace("_", "%20")
                county = replace_strs(county)

                state_abbr = county_row["stateabbr"].lower()
                state = county_row["state"].lower()

                base_url = "https://www.redfin.com/stingray/do/location-autocomplete?location="
                county_arg = f"{county}%2C%20{state_abbr}"
                tail_url = f"&start=0&count=10&v=2&market={state}&al=1&iss=false&ooa=true&mrs=false"

                fetch_url = f"{base_url}{county_arg}{tail_url}"

                SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")
                SCRAPERAPI_URL = "https://api.scraperapi.com"
                params = {
                    "api_key": SCRAPER_API_KEY,
                    "url": fetch_url
                }

                resp = request_with_retry(SCRAPERAPI_URL, params)

                try:
                    json_prep = resp.text.replace("{}&&", "")
                    data = json.loads(json_prep)
                except Exception as e:
                    print(e)
                    county_row["error"] = e
                    problem_counties.append(county_row)

                # Validate the county
                input_county = county.replace("%20", " ").title()
                input_state_abbr = county_row["stateabbr"]

                redfin_county_data = data["payload"]["sections"][0]["rows"][0]
                redfin_county = redfin_county_data["name"].title()
                redfin_state_abbr = redfin_county_data["subName"].split(",")[0]

                counties_match = input_county == redfin_county
                state_abbr_match = input_state_abbr == redfin_state_abbr
                both_match = counties_match and state_abbr_match

                updated_county_dict = county_row
                if both_match:
                    active_on_redfin = data["payload"]["sections"][0]["rows"][0]["active"]
                    if not active_on_redfin:
                        updated_county_dict["active_on_redfin"] = False
                        updated_county_dict["redfin_url"] = None
                    else:
                        redfin_base_url = "https://redfin.com"
                        redfin_county_rel_url = data["payload"]["sections"][0]["rows"][0]["url"]
                        tail_url = "/filter/property-type=land"
                        redfin_county_url = f"{redfin_base_url}{redfin_county_rel_url}{tail_url}"
                        updated_county_dict["active_on_redfin"] = True
                        updated_county_dict["redfin_url"] = redfin_county_url
                else:
                    print("both_match =", both_match)
                    problem_counties.append(updated_county_dict)

                checked_counties.append(updated_county_dict)
            except Exception as error:
                print(error)
                county_row["error"] = error
                problem_counties.append(county_row)
        pdb.set_trace()
        return checked_counties

    counties = get_counties()
    pdb.set_trace()
    redfin_checked_counties = get_county_urls(counties)

    export_df = pd.DataFrame(redfin_checked_counties)
    export_df.to_csv("redfin_checked_counties.csv", index=False)


if __name__ == "__main__":
    check_redfin_counties()
