import os
import csv
import re
from seleniumbase import SB
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import time
import random

with SB(uc=True, test=True, incognito=True, locale_code="en") as sb:
    link = "https://www.tripadvisor.com/Attraction_Review-g60713-d102523-Reviews-Alcatraz_Island-San_Francisco_California.html"
    filter_button = 'span:contains("Filters")'

    sb.uc_open_with_reconnect(link)  # Handle bot-check later
    sb.reconnect(0.1)

    # Wait until the filter button is visible and click
    sb.uc_click(filter_button, reconnect_time=4)
    print("Filter Button Clicked")

    rating_button = ".qgcDG > div:nth-child(2) > div > button:nth-of-type(2)"
    # print(len(rating_button))
    print(rating_button)
    sb.uc_click(rating_button, reconnect_time=4)
    print("Rating element clicked")


    # Apply button (use more stable 'Apply' button locator)
    apply_button = 'span:contains("Apply")'
    print("Apply Button Clicked")
    # sb.wait_for_element_visible(apply_button, timeout=10)
    sb.uc_click(apply_button, reconnect_time=4)

