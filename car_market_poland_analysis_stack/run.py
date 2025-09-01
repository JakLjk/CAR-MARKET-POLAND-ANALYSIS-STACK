from datetime import date

# from car_market_poland_analysis_stack.scraping.vehicles import get_vehicle_information
# from car_market_poland_analysis_stack.scraping.dictionaries import get_dictionary, Dictionary

from car_market_poland_analysis_stack.scraping.vehicles.vehicles import get_vehicles, write_ndjson
from car_market_poland_analysis_stack.scraping.dictionaries.dictionaries import get_dictionary, Dictionary, write_dictionary


if __name__=="__main__":
    # # get_vehicle_information(date(2025,8,20), date(2025,8,22), "04", save_to_file="test_veh2.json")
    # get_dictionary(Dictionary.WOJEWODZTWA, save_to_file="test_woj.json")
    # # print(get_vehicle_information(date(2025,8,20), date(2025,8,22), "04"))
    # # print(get_dictionary(Dictionary.WOJEWODZTWA))
    # iterator = get_vehicles(
    #     date(2025, 8,20),
    #     date(2025,8,30),
    #     "04",

    # )
    # write_ndjson("test.json", iterator)
    HDFS_PATH = "hdfs://192.168.0.12:8020/cepik/bronze/test.json"
    d = get_dictionary(Dictionary.WOJEWODZTWA)
    # write_dictionary(HDFS_PATH, d)
    # write_dictionary_hdfs_webhdfs(
    #     "192.168.0.12",
    #     9870,
    #     "/cepik/bronze/wojewodztwa.json",
    #     d,
    #     user="thinkpad"
    # )