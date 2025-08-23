import os
import csv
import re
from seleniumbase import SB
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import time
import random


def extract_location_name(url):
    # Extract location name from the URL (between the last two dashes)
    match = re.search(r'-Reviews-(.*?)-', url)
    if match:
        location_name = match.group(1).replace('_', ' ')
        return location_name.strip()
    return 'Unknown_Location'


def save_reviews_to_csv(reviews, location_name):
    # Create 'reviews' folder if it doesn't exist
    if not os.path.exists('venv/reviews'):
        os.makedirs('venv/reviews')

    # Define the path to save the CSV file (with the location name)
    file_path = os.path.join('venv/reviews', f'{location_name}.csv')

    # Define the CSV headers
    headers = ['Review ID', 'Review Link', 'Review Title', 'Ratings Score', 'Review Date', 'Review Body', 'Username',
               'Username Link']

    # Write reviews to CSV
    with open(file_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for review in reviews:
            writer.writerow({
                'Review ID': review.get('review_id'),
                'Review Link': review['review_link_href'],
                'Review Title': review['review_title'],
                'Ratings Score': review['ratings_score'],
                'Review Date': review['review_date'],
                'Review Body': review['review_body'],
                'Username': review['username'],
                'Username Link': review['username_link'],
            })
    print(f"Reviews saved to {file_path}")


def extract_reviews(url, limit=10):
    reviews_data = []
    count = 0
    page_number = 1  # Track the current page number

    with SB(uc=True, demo=True, incognito=True, locale_code="en") as sb:
        sb.uc_open_with_reconnect(url, 5)
        time.sleep(random.uniform(4, 8))
        while count < limit:
            get_reviews = sb.find_element(By.CLASS_NAME, "LbPSX")
            get_each_review_child = get_reviews.find_elements(By.CLASS_NAME, "_c")

            for element in get_each_review_child:
                review_data = {}

                review_link = element.find_element(By.CSS_SELECTOR, ".biGQs._P.fiohW.qWPrE.ncFvv.fOtGX")
                review_url = review_link.find_element(By.XPATH, "./*").get_attribute("href")
                review_data['review_link_href'] = review_url
                review_data['review_title'] = review_link.find_element(By.XPATH, "./*").text

                # Extract review_id from the review URL
                review_id_match = re.search(r"-r(\d+)-", review_url)
                if review_id_match:
                    review_data['review_id'] = review_id_match.group(1)

                ratings = element.find_element(By.CSS_SELECTOR, ".UctUV.d.H0")
                review_data['ratings_score'] = ratings.find_element(By.XPATH, "./*").text.split(" ")[0].strip()

                review_data['review_date'] = element.find_element(By.CLASS_NAME, "RpeCd").text.strip().split('•')[
                    0].strip()

                username_entities = element.find_element(By.CSS_SELECTOR, ".biGQs._P.fiohW.fOtGX")
                review_data['username'] = username_entities.text
                review_data['username_link'] = username_entities.find_element(By.CSS_SELECTOR,
                                                                              ".BMQDV._F.Gv.wSSLS.SwZTJ.FGwzt.ukgoS").get_attribute(
                    "href")

                review_data['review_body'] = element.find_element(By.CLASS_NAME, "JguWG").text.strip()

                reviews_data.append(review_data)
                count += 1

                if count >= limit:
                    break

            # Break if we've reached the review limit
            if count >= limit:
                break

            try:
                # Pagination Logic: First Page vs Subsequent Pages
                next_button_divs = sb.find_elements(By.CLASS_NAME, "UCacc")
                if page_number == 1:
                    # First page: select the first UCacc div
                    next_button = next_button_divs[0].find_element(By.XPATH, "./*")
                else:
                    # Second page and beyond: select the second UCacc div if there are 3 UCacc divs
                    if len(next_button_divs) >= 3:
                        next_button = next_button_divs[1].find_element(By.XPATH, "./*")
                    else:
                        next_button = next_button_divs[0].find_element(By.XPATH, "./*")

                next_page_url = next_button.get_attribute("href")

                if not next_page_url or 'disabled' in next_button.get_attribute("class"):
                    print("No more pages to navigate.")
                    break

                page_number += 1
                sb.uc_open_with_reconnect(next_page_url, 8)
                time.sleep(random.uniform(4, 8))  # Add a delay to prevent detection

            except NoSuchElementException:
                print("Next button not found.")
                break

    return reviews_data


# Usage
link = "https://www.tripadvisor.com/Attraction_Review-g60713-d104675-Reviews-Golden_Gate_Bridge-San_Francisco_California.html"
location_name = extract_location_name(link)  # Extract location name from URL
reviews = extract_reviews(link, limit=1000)  # Extract reviews with a specified limit

save_reviews_to_csv(reviews, location_name)  # Save reviews to CSV
