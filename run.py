from datetime import date

# from car_market_poland_analysis_stack.scraping.vehicles import get_vehicle_information
# from car_market_poland_analysis_stack.scraping.dictionaries import get_dictionary, Dictionary

from car_market_poland_analysis_stack.scraping.vehicles import iter_vehicles, write_ndjson

if __name__=="__main__":
    # # get_vehicle_information(date(2025,8,20), date(2025,8,22), "04", save_to_file="test_veh2.json")
    # get_dictionary(Dictionary.WOJEWODZTWA, save_to_file="test_woj.json")
    # # print(get_vehicle_information(date(2025,8,20), date(2025,8,22), "04"))
    # # print(get_dictionary(Dictionary.WOJEWODZTWA))
    iterator = iter_vehicles(
        date(2025, 8,20),
        date(2025,8,30),
        "04",

    )
    write_ndjson("test.json", iterator)
