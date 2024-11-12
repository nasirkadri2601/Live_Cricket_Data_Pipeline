import requests
from bs4 import BeautifulSoup
import json
import boto3
from datetime import datetime

# Initialize S3 client
s3_client = boto3.client('s3')

def extract_rapidapi_data(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        data = response.json()

        if data['status']:
            # Extract relevant match information from the first match data['data'][0]. If we wanted to extract data from second match then replace [0] with[1]
            match_info = data['data'][1]

            # Create a dictionary with the specified fields
            output = {
                "venue": match_info.get("venue"),
                "series_id": match_info.get("series_id"),
                "match_id": match_info.get("match_id"),
                "series": match_info.get("series"),
                "venue_id": match_info.get("venue_id"),
                "match_status": match_info.get("match_status"),
                "toss": match_info.get("toss"),
                "matchs": match_info.get("matchs"),
                "match_time": match_info.get("match_time"),
                "match_type": match_info.get("match_type"),
                "match_date": match_info.get("match_date"),
                "series_type": match_info.get("series_type"),
                "balling_team": match_info.get("balling_team"),
                "batting_team": match_info.get("batting_team"),

                "team_a_id": match_info.get("team_a_id"),
                "team_a_img": match_info.get("team_a_img"),
                "team_a_scores_over": match_info.get("team_a_scores_over"),
                "team_a_over": match_info.get("team_a_over"),
                "team_a": match_info.get("team_a"),
                "team_a_scores": match_info.get("team_a_scores"),
                "team_a_short": match_info.get("team_a_short"),
                "team_a_score": match_info.get("team_a_score"),

                "team_b_id": match_info.get("team_b_id"),
                "team_b_img": match_info.get("team_b_img"),
                "team_b_score": match_info.get("team_b_score"),
                "team_b_scores_over": match_info.get("team_b_scores_over"),
                "team_b_over": match_info.get("team_b_over"),
                "team_b_short": match_info.get("team_b_short"),
                "team_b": match_info.get("team_b"),
                "team_b_scores": match_info.get("team_b_scores"),

                "date_time": match_info.get("date_time"),
                "trail_lead": match_info.get("trail_lead"),
                "session": match_info.get("session"),
                "need_run_ball": match_info.get("need_run_ball"),
                "result": match_info.get("result")
            }

            return output
        else:
            return None
    except requests.exceptions.RequestException as e:
        return None
    except Exception as e:
        return None


def extract_espncricinfo_data(url):
    try:
        # Fetch webpage content
        res = requests.get(url)
        res.raise_for_status()  # Raise an exception for non-2xx status codes
        soup = BeautifulSoup(res.text, 'html.parser')

        # Extract match details
        match_details = {}

        # Extract specific details
        info = soup.find_all("td", attrs={'class': 'ds-min-w-max'})
        details = [item.text.strip() for item in info]
        match_details.update({index: text for index, text in enumerate(details)})

        # Remove unnecessary details
        keys_to_remove = [6, 7, 14, 15, 22, 23, 24, 25] + list(range(32, len(match_details)))
        for key in keys_to_remove:
            match_details.pop(key, None)

        # Extract run rates and commentary
        run_rates_raw = soup.find_all("div", attrs={'class': 'ds-text-tight-s ds-font-regular ds-overflow-x-auto ds-scrollbar-hide ds-whitespace-nowrap ds-mt-1 md:ds-mt-0 lg:ds-flex lg:ds-items-center lg:ds-justify-between lg:ds-px-4 lg:ds-py-2 lg:ds-bg-fill-content-alternate ds-text-typo-mid3 md:ds-text-typo-mid2'})[0].text
        run_rates_cleaned = run_rates_raw.replace("\xa0", ",").replace("•,", " ").replace(":,", ":")
        match_details["run_rates"] = run_rates_cleaned

        commentary = soup.find_all("div", attrs={'class': 'ds-text-tight-m ds-font-regular ds-flex ds-px-3 ds-py-2 lg:ds-px-4 lg:ds-py-[10px] ds-items-start ds-select-none lg:ds-select-auto'})[0].text
	#sometime the commentary class got changed so I have included both piece of code. If the above code doesnt work replace with the below one
	#commentary = soup.find_all("div", attrs={'class': 'ds-text-tight-m ds-font-regular ds-flex ds-px-3 ds-py-2 lg:ds-px-4 lg:ds-py-[10px] ds-items-start ds-select-none lg:ds-select-auto ds-items-center'})[0].text
        match_details["commentary"] = commentary

        return match_details

    except requests.exceptions.RequestException as e:
        return None
    except Exception as e:
        return None


def lambda_handler(event, context):
    # RapidAPI credentials
    headers = {
        "x-rapidapi-key": "", #Put your key here
        "x-rapidapi-host": "cricket-live-line1.p.rapidapi.com"
    }

    # RapidAPI URL
    url = "https://cricket-live-line1.p.rapidapi.com/home"

    # ESPN Cricinfo URL. Put live match URL only
    espn_url = ''

    # Fetch data from both sources
    rapidapi_data = extract_rapidapi_data(url, headers)
    espn_data = extract_espncricinfo_data(espn_url)

    if rapidapi_data and espn_data:
        # Combine data from both sources
        combined_data = {
            "match_details": rapidapi_data,
            "player_details": espn_data
        }

        # Generate a unique file name with date and timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f"raw_data/to_processed/cricket_data_raw_{timestamp}.json"

        # Upload to S3
        try:
            s3_client.put_object(
                Bucket="cricket-data-pipeline",
                Key=file_name,
                Body=json.dumps(combined_data),
                ContentType="application/json"
            )
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Data successfully uploaded to S3', 'file': file_name})
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': f'Failed to upload to S3: {str(e)}'})
            }

    else:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Failed to fetch data from one or more sources'})
        }
