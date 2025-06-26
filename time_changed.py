import pandas as pd
import numpy as np
import re
import math
import calendar
from datetime import datetime, timedelta

# def parse_visibility(value):
#     """Parses visibility value, ensuring it's a valid number."""
#     if value.isdigit():
#         return int(value)
#     elif value in ['KT', 'MPS', 'VRB']:
#         return 999  # For invalid visibility, return a default value (999)
#     return int(value)  # Fallback for other valid formats
# # def parse_wind(value):
#     """Parses wind value, ensuring it's a valid number."""
#     if value.isdigit():

def parse_temp(value):
    """Parses temperature values, handling 'M' for negative values."""
    if value.startswith("M"):
        return -int(value[1:]) if value[1:] != "00" else 0  # Convert "M00" to 0
    return int(value)

def is_leap_year(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

def parse_metar_data(input_file, output_file):

    df_input = pd.read_excel(input_file)

    df_input['Timestamp'] = pd.to_datetime(df_input['Timestamp'])  # Ensures it’s datetime format

    df_input['Timestamp'] = pd.to_datetime(df_input['Timestamp'], format='%d-%m-%Y %I.%M.%S %p', errors='coerce')

    # Clean METAR Data
    df_input['Cleaned_METAR'] = df_input['METAR Data'].apply(
        lambda x: re.sub(r"\s*METAR:\s*", "", str(x)).strip() if pd.notnull(x) else ""
    )

    for index, row in df_input.iterrows():
    
        timestamp = row['Timestamp']

        metar_line = row['METAR Data']
        
        metar_lines = df_input[['Timestamp', 'Cleaned_METAR']].dropna().values.tolist()

        pattern = (
        r"METAR (\w+) (\d{6}Z) "
        r"(?:(\d{3}|VRB)(\d{2})(G\d{2,3})?(KT|MPS|KMH)|00000KT) "
        r"(?:(\d{4}))? "  # Visibility
        r"(?:R\d{2}[LRC]?/P?M?\d+(?:V\d+)?(?:[UDN])?\s*)?"  # Runway info
        r"((?:\w{2,6}\s?)*)"  # Weather phenomena (like BR, HZ, etc.)
        r"(?:(?:SKC|FEW|SCT|BKN|OVC|NSC)\d{3}\s?)* "  # Cloud information 
        r"(-?\d+|M\d+)/(-?\d+|M\d+) "  # Temperature/Dew point
        r"Q(\d+)"  # QNH
        r"(?:\s*\d{3}V\d{3})?"  # Variable wind direction
        r"(?:\s*(.*))"  # Remaining remarks
        )

        data_list = []

        unmatched_metars = []

        for timestamp,metar_line in metar_lines:
            # Ignore Rxx/Pxxxx if present
            metar_line = re.sub(r"R\d{2}/P\d+", "", metar_line)

            # Extract date-time part
            date_time_match = re.search(r"(\d{6}Z)", metar_line)
            date_time_raw = date_time_match.group(1) if date_time_match else "999999Z"
            
            year = timestamp.year
            month = timestamp.month

            # Convert to readable format
            day = int(date_time_raw[:2])
            hour = int(date_time_raw[2:4])
            minute = int(date_time_raw[4:6])

            # Handle date validation - IMPORTANT FIX
            try:
                # Try creating the datetime with the month from the timestamp
                original_time = datetime(year, month, day, hour, minute)
            except ValueError:
                # If that fails, try using the previous or next month
                print(f"Date validation error for {day} in month {month}. Trying alternative month.")

                if month == 2:
                    if not is_leap_year(year) and day > 28:  # Non-leap year
                        day = 28
                    elif is_leap_year(year) and day > 29:  # Leap year
                        day = 29

                elif month in [4, 6, 9, 11] and day > 30:  # Months with 30 days
                    day = 30

                elif month in [1, 3, 5, 7, 8, 10, 12] and day > 31:  # Months with 31 days
                    day = 31

                elif day > 31:  # Fallback (invalid date for any month)
                    day = 28  # Safe fallback
                try:
                    original_time = datetime(year, month, day, hour, minute)
                except ValueError:
                    # If still failing, use the current date from timestamp
                    print(f"Still having issues with date {day}-{month}-{year}, using timestamp date instead")
                    original_time = datetime(timestamp.year, timestamp.month, timestamp.day, hour, minute)
            
            adjusted_time = original_time + timedelta(hours=5, minutes=30)
            formatted_datetime = adjusted_time.strftime("%d-%m-%Y %H.%M")
            time_24hr = adjusted_time.strftime("%H:%M")

            # Handle "DATA UNAVAILABLE" case
            if "DATA UNAVAILABLE" in metar_line:
                print(f"Filling missing data for: {metar_line}")
                data_list.append({
                    "DATETIME": formatted_datetime, "YEAR": year, "MONTH": month, "DD": adjusted_time.day, "GGGG": time_24hr,
                    "DDD": 999, "FF": 999, "VV": 999, "WW": 999, "N": 999, "TTT": 999, "TDTD": 999, "RH": 999,
                    "QFE": 999, "QNH": 999, "U": 999, "V": 999, "Wx": 999, "Wy": 999, "Low_Visibility_Indicator": 999,
                    "Daylight_Indicator": 999, "Dew_Point_Depression": 999, "Remark": "DATA UNAVAILABLE"
                })
                continue  

            match = re.match(pattern, metar_line)
            if match:
                station = match.group(1)
                wind_direction = match.group(3) if match.group(3) else "000"
                wind_speed = int(match.group(4)) if match.group(4) else 0
                wind_gust = int(match.group(5)[1:]) if match.group(5) else 0
                visibility = int(match.group(7))

                weather_code_map = {
                    "BR": 10,   # Mist
                    "HZ": 5,    # Haze
                    "DU": 7,    # Widespread Dust
                    "FU": 4,    # Smoke
                    "BLDU": 9,  # Blowing Widespread Dust
                    "DZ": 50,   # Drizzle
                    "RA": 21,   # Rain
                    "SN": 22,   # Snow
                    "SG": 20,   # Snow Grains
                    "IC": 15,   # Ice Crystals NN
                    "PL": 79,   # Ice Pellets
                    "GR": 27,   # Hail
                    "GS": 87,   # Small Hail and/or Snow Pellets
                    "UP": 19,   # Unknown Precipitation NN
                    "FG": 28,   # Fog
                    "VA": 4,   # Volcanic Ash
                    "SA": 22,   # Sand
                    "SS": 23,   # Sandstorm
                    "DS": 24,   # Duststorm
                    "PO": 8,   # Dust/Sand Whirls
                    "SQ": 18,   # Squalls
                    "FC": 19,   # Funnel Clouds
                    "TS": 29,   # Thunderstorm
                    "SH": 70,   # Showers
                    "FZ": 24,   # Freezing
                    "DR": 36,   # Low Drifting
                    "MI": 12,   # Shallow
                    "BC": 11,   # Patches
                    "PR": 34,   # Partial
                    "BL": 38,   # Blowing
                    "VC": 36    # Vicinity
                }

                # Initialize weather1 and weather2 to empty strings in the special case handling
                weather1 = ""
                weather2 = ""

                # Define the list of weather codes to check
                weather_codes = list(weather_code_map.keys())

                # Extract weather condition details if present
                weather_match = re.findall(r"\b(" + "|".join(weather_codes) + r")\b", metar_line)
                if weather_match:
                    weather1 = weather_match[0]
                    if len(weather_match) > 1:
                        weather2 = weather_match[1]

                # Define a function to map weather codes to WW values using the weather_code_map
                def get_ww(weather1, weather2):
                    ww = 999  # Default value if no match found
                    if weather1 in weather_code_map:
                        ww = weather_code_map[weather1]
                    if weather2 in weather_code_map:
                        ww = weather_code_map[weather2]  # If two weather conditions are found, use the second one
                    return ww

                # Assign WW column safely using the highest priority (lowest number)
                codes_to_check = [weather1, weather2]
                ww = min([weather_code_map.get(code, 999) for code in codes_to_check])

                # Improved cloud condition handling
                cloud_condition = weather1 if weather1 in ["OVC", "BKN", "SCT", "FEW", "SKC","NSC"] else weather2
                cloud_condition = cloud_condition or ""  # Ensure it's always a string

                temp = parse_temp(match.group(9))
                dew_point = parse_temp(match.group(10))
                qnh = int(match.group(11))
                remarks = match.group(12) or ""

                # Initialize cloud_condition to an empty string
                cloud_condition = ""
                # Extract cloud conditions if present
                cloud_match = re.findall(r"\b(OVC|BKN|SCT|FEW|SKC|NSC)(?:\d{3})?\b", metar_line)
                cloud_conditions = ["OVC", "BKN", "SCT", "FEW", "SKC","NSC"]
                if cloud_match:

                    # Check the cloud conditions in order of priority
                    for condition in cloud_conditions:
                        if condition in cloud_match:
                            cloud_condition = condition
                            break   # Stop once the highest priority condition is found

                # Assign cloud cover safely
                cloud_cover = 8 if "OVC" in cloud_condition else \
                6 if "BKN" in cloud_condition else \
                4 if "SCT" in cloud_condition else \
                3 if "FEW" in cloud_condition else \
                1 if "SKC" in cloud_condition else \
                0 if "NSC" in cloud_condition else 999  # 999 if no matching condition

                # RH Calculation (Avoid Division by Zero)
                def saturation_vapor_pressure(temp):
                    return 6.11 * 10 ** ((7.5 * temp) / (237.3 + temp))  

                e_t = saturation_vapor_pressure(temp)
                e_td = saturation_vapor_pressure(dew_point)
                rh = round((e_td / e_t) * 100) if e_t != 0 else 0

                # Calculate QFE
                height = 70  
                qfe = round(qnh * math.exp(-height / (29.3 * (temp + 273.15))))

                # Additional calculations
                low_visibility_indicator = int(visibility < 1500)
                daylight_indicator = int(6 <= adjusted_time.hour < 18)
                dew_point_depression = temp - dew_point

                # Wind components
                wind_direction_deg = 999 if wind_direction == "VRB" else int(wind_direction)
                u = -wind_speed * np.sin(np.radians(wind_direction_deg))
                v = -wind_speed * np.cos(np.radians(wind_direction_deg))
                wx = u * np.cos(np.radians(wind_direction_deg))
                wy = v * np.sin(np.radians(wind_direction_deg))

                data_list.append({
                    "DATETIME": formatted_datetime,
                    "YEAR": year,
                    "MONTH": month,
                    "DD": adjusted_time.day,
                    "GGGG": time_24hr,
                    "DDD": wind_direction,
                    "FF": wind_speed,
                    "VV": visibility,
                    "WW": ww,
                    "N": cloud_cover,
                    "TTT": temp,
                    "TDTD": dew_point,
                    "RH": rh,
                    "QFE": qfe,
                    "QNH": qnh,
                    "U": u,
                    "V": v,
                    "Wx": wx,
                    "Wy": wy,
                    "Low_Visibility_Indicator": low_visibility_indicator,
                    "Daylight_Indicator": daylight_indicator,
                    "Dew_Point_Depression": dew_point_depression,
                    "Remark": remarks
                })
            else:
                unmatched_metars.append(metar_line)
                print(f"Handling special case for: {metar_line}")
                # Extract wind direction (DDD)
                wind_match = re.search(r"(VRB|\d{3})(\d{2})G?(\d{2})?KT", metar_line) #r"(VRB|\d{3})(\d{2})KT(?:G(\d{2})KT)?" #(\d{3})(\d{2})G?(\d{2})?\D*KT" 

                if wind_match:
                    wind_direction = (wind_match.group(1))
                    wind_speed = int(wind_match.group(2))
                    wind_gust = int(wind_match.group(3)) if wind_match.group(3) else 0
                else:
                    wind_direction, wind_speed, wind_gust = 999, 999, 0

                visibility_match = re.search(r"\s(\d{4})\s", metar_line)
                visibility = int(visibility_match.group(1)) if visibility_match else 999  

                weather_code_map = {
                    "BR": 10,   # Mist
                    "HZ": 5,    # Haze
                    "DU": 7,    # Widespread Dust
                    "FU": 4,    # Smoke
                    "BLDU": 9,  # Blowing Widespread Dust
                    "DZ": 50,   # Drizzle
                    "RA": 21,   # Rain
                    "SN": 22,   # Snow
                    "SG": 20,   # Snow Grains
                    "IC": 15,   # Ice Crystals NN
                    "PL": 79,   # Ice Pellets
                    "GR": 27,   # Hail
                    "GS": 87,   # Small Hail and/or Snow Pellets
                    "UP": 19,   # Unknown Precipitation NN
                    "FG": 28,   # Fog
                    "VA": 4,   # Volcanic Ash
                    "SA": 22,   # Sand
                    "SS": 23,   # Sandstorm
                    "DS": 24,   # Duststorm
                    "PO": 8,   # Dust/Sand Whirls
                    "SQ": 18,   # Squalls
                    "FC": 19,   # Funnel Clouds
                    "TS": 29,   # Thunderstorm
                    "SH": 70,   # Showers
                    "FZ": 24,   # Freezing
                    "DR": 36,   # Low Drifting
                    "MI": 12,   # Shallow
                    "BC": 11,   # Patches
                    "PR": 34,   # Partial
                    "BL": 38,   # Blowing
                    "VC": 36    # Vicinity
                }

                # Initialize weather1 and weather2 to empty strings in the special case handling
                weather1 = ""
                weather2 = ""

                # Define the list of weather codes to check
                weather_codes = list(weather_code_map.keys())

                # Extract weather condition details if present
                weather_match = re.findall(r"\b(" + "|".join(weather_codes) + r")\b", metar_line)
                if weather_match:
                    weather1 = weather_match[0]
                    if len(weather_match) > 1:
                        weather2 = weather_match[1]

                # Define a function to map weather codes to WW values using the weather_code_map
                def get_ww(weather1, weather2):
                    ww = 999  # Default value if no match found
                    if weather1 in weather_code_map:
                        ww = weather_code_map[weather1]
                    if weather2 in weather_code_map:
                        ww = weather_code_map[weather2]  # If two weather conditions are found, use the second one
                    return ww

                # Assign WW column safely using the highest priority (lowest number)
                codes_to_check = [weather1, weather2]
                ww = min([weather_code_map.get(code, 999) for code in codes_to_check])


                # Improved cloud condition handling
                cloud_condition = weather1 if weather1 in ["SKC", "FEW", "SCT", "BKN", "OVC","NSC"] else weather2
                cloud_condition = cloud_condition or ""  # Ensure it's always a string


                temp_match = re.search(r"(-?\d+|M\d+)/(-?\d+|M\d+)", metar_line)
                if temp_match:
                    temp = parse_temp(temp_match.group(1))
                    dew_point = parse_temp(temp_match.group(2))
                else:
                    temp, dew_point = 999, 999


                # Extract QNH (Pressure)
                qnh_match = re.search(r"Q(\d+)", metar_line)
                qnh = int(qnh_match.group(1)) if qnh_match else 999

                
                # Initialize cloud_condition to an empty string
                cloud_condition = ""
                # Extract cloud conditions if present
                cloud_match = re.findall(r"\b(OVC|BKN|SCT|FEW|SKC|NSC)(?:\d{3})?\b", metar_line)
                cloud_conditions = ["OVC", "BKN", "SCT", "FEW", "SKC","NSC"]
                if cloud_match:

                    # Check the cloud conditions in order of priority
                    for condition in cloud_conditions:
                        if condition in cloud_match:
                            cloud_condition = condition
                            break   # Stop once the highest priority condition is found

                # Assign cloud cover safely
                cloud_cover = 8 if "OVC" in cloud_condition else \
                6 if "BKN" in cloud_condition else \
                4 if "SCT" in cloud_condition else \
                3 if "FEW" in cloud_condition else \
                1 if "SKC" in cloud_condition else \
                0 if "NSC" in cloud_condition else 999  # 999 if no matching condition

                
                # RH Calculation (Avoid Division by Zero)
                def saturation_vapor_pressure(temp):
                    return 6.11 * 10 ** ((7.5 * temp) / (237.3 + temp))  

                e_t = saturation_vapor_pressure(temp)
                e_td = saturation_vapor_pressure(dew_point)
                rh = round((e_td / e_t) * 100) if e_t != 0 else 0

                # Calculate QFE
                height = 70  
                qfe = round(qnh * math.exp(-height / (29.3 * (temp + 273.15))))

                # Additional calculations
                low_visibility_indicator = int(visibility < 1500)
                daylight_indicator = int(6 <= adjusted_time.hour < 18)
                dew_point_depression = temp - dew_point

                # Wind components
                wind_direction_deg = 999 if wind_direction == "VRB" else int(wind_direction)
                u = -wind_speed * np.sin(np.radians(wind_direction_deg))
                v = -wind_speed * np.cos(np.radians(wind_direction_deg))
                wx = u * np.cos(np.radians(wind_direction_deg))
                wy = v * np.sin(np.radians(wind_direction_deg))

                # Extract everything after QNH as Remarks
                remarks_match = re.search(r"Q\d+\s*(.*)", metar_line)
                remarks = remarks_match.group(1).strip() if remarks_match else ""

                data_list.append({
                    "DATETIME": formatted_datetime,
                    "YEAR": year,
                    "MONTH": month,
                    "DD": adjusted_time.day,
                    "GGGG": time_24hr,
                    "DDD": wind_direction, 
                    "FF": wind_speed,  
                    # "GUST": wind_gust if wind_gust else 0,
                    "VV": visibility,
                    "WW": ww,
                    "N": cloud_cover,
                    "TTT": temp,
                    "TDTD": dew_point,
                    "RH": rh,
                    "QFE": qfe,
                    "QNH": qnh,  
                    "U": u,
                    "V": v,
                    "Wx": wx,
                    "Wy": wy,
                    "Low_Visibility_Indicator": low_visibility_indicator,
                    "Daylight_Indicator": daylight_indicator,
                    "Dew_Point_Depression": dew_point_depression,
                    "Remark": remarks 
                })
                    

        # Create DataFrame
        df_output = pd.DataFrame(data_list)
        df_output.to_excel(output_file, index=False)
        print(f"Data successfully extracted and saved to {output_file}")

        if unmatched_metars:
            df_unmatched = pd.DataFrame({"METAR": unmatched_metars})
            df_unmatched.to_excel("unparsed_metars.xlsx", index=False)
            print(f"Unparsed METARs saved to unparsed_metars.xlsx")



if __name__ == "__main__":
    print("Running METAR parser...")
    parse_metar_data("METAR_VISR_data.xlsx", "METAR_VISR_data1.xlsx")
