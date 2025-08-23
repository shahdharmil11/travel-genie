import os
import csv
import re
from seleniumbase import SB
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
import time
import random


def extract_location_name(url):
    # Extract location name from the URL (between the last two dashes)
    match = re.search(r'-Reviews-(.*?)-', url)
    if match:
        location_name = match.group(1).replace('_', ' ')
        return location_name.strip()
    return 'Unknown_Location'

def extract_location_id(url):
    # Extract location name from the URL (between the last two dashes)
    match = re.search(r'-d(\d+)-', url)
    if match:
        location_id = match.group(1)
        return location_id
    return 'Unknown_Location'

def save_reviews_to_csv(reviews, location_name, location_id):
    # Create 'reviews' folder if it doesn't exist
    if not os.path.exists('dmg7374/reviews'):
        os.makedirs('dmg7374/reviews')

    # Define the path to save the CSV file (with the location name)
    file_path = os.path.join('dmg7374/reviews', f'{location_name}.csv')

    # Define the CSV headers
    headers = ['Review ID', 'Review Link', 'Review Title', 'Ratings Score', 'Review Date', 'Review Body', 'Username',
               'Username Link']

    # Write reviews to CSV
    with open(file_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for review in reviews:
            writer.writerow({
                'Attraction ID': location_id,
                'Attraction Name': location_name,
                'Review ID': review.get('review_id'),
                'Review Link': review['review_link_href'],
                'Review Title': review['review_title'],
                'Ratings Score': review['rating_score'],
                'Review Date': review['review_date'],
                'Review Body': review['review_body'],
                'Username': review['username'],
                'Username Link': review['username_link'],
            })
    print(f"Reviews saved to {file_path}")

# def save_reviews_to_csv(reviews):
#     # Create 'reviews' folder if it doesn't exist
#     if not os.path.exists('dmg7374/reviews'):
#         os.makedirs('dmg7374/reviews')
#
#     # Define the path to save the CSV file
#     file_path = os.path.join('dmg7374/reviews', 'all_reviews.csv')
#
#     # Define the CSV headers
#     headers = ['Review ID', 'Review Link', 'Review Title', 'Ratings Score', 'Review Date', 'Review Body', 'Username',
#                'Username Link']
#
#     # Write reviews to CSV
#     with open(file_path, 'w', newline='', encoding='utf-8') as file:
#         writer = csv.DictWriter(file, fieldnames=headers)
#         writer.writeheader()
#         for review in reviews:
#             writer.writerow({
#                 'Review ID': review.get('review_id'),
#                 'Review Link': review['review_link_href'],
#                 'Review Title': review['review_title'],
#                 'Ratings Score': review['ratings_score'],
#                 'Review Date': review['review_date'],
#                 'Review Body': review['review_body'],
#                 'Username': review['username'],
#                 'Username Link': review['username_link'],
#             })
#     print(f"Reviews saved to {file_path}")


# def extract_reviews(url, rating=1, limit=100):
#     reviews_data = []
#     count = 0
#     page_number = 1  # Track the current page number
#
#     filter_button = 'span:contains("Filters")'
#     rating_button = f".qgcDG > div:nth-child(2) > div > button:nth-of-type({rating})"
#     apply_button = 'span:contains("Apply")'
#
#     with SB(uc=True, demo=True, incognito=True, locale_code="en") as sb:
#         sb.uc_open_with_reconnect(url, 5)
#         sb.uc_click(filter_button, reconnect_time=2)
#         sb.uc_click(rating_button, reconnect_time=2)
#         sb.uc_click(apply_button, reconnect_time=2)
#         time.sleep(random.uniform(3, 6))
#
#         while count < limit:
#             get_reviews = sb.find_element(By.CLASS_NAME, "LbPSX")
#             get_each_review_child = get_reviews.find_elements(By.CLASS_NAME, "_c")
#
#             for element in get_each_review_child:
#                 try:
#                     review_data = {}
#                     review_link = element.find_element(By.CSS_SELECTOR, ".biGQs._P.fiohW.qWPrE.ncFvv.fOtGX")
#                     review_url = review_link.find_element(By.XPATH, "./*").get_attribute("href")
#                     review_data['review_link_href'] = review_url
#                     review_data['review_title'] = review_link.find_element(By.XPATH, "./*").text
#
#                     # Extract review_id from the review URL
#                     review_id_match = re.search(r"-r(\d+)-", review_url)
#                     if review_id_match:
#                         review_data['review_id'] = review_id_match.group(1)
#
#                     ratings = element.find_element(By.CSS_SELECTOR, ".UctUV.d.H0")
#                     review_data['ratings_score'] = ratings.find_element(By.XPATH, "./*").text.split(" ")[0].strip()
#
#                     review_data['review_date'] = element.find_element(By.CLASS_NAME, "RpeCd").text.strip().split('•')[0].strip()
#
#                     username_entities = element.find_element(By.CSS_SELECTOR, ".biGQs._P.fiohW.fOtGX")
#                     review_data['username'] = username_entities.text
#                     review_data['username_link'] = username_entities.find_element(By.CSS_SELECTOR,
#                                                                               ".BMQDV._F.Gv.wSSLS.SwZTJ.FGwzt.ukgoS").get_attribute(
#                         "href")
#
#                     review_data['review_body'] = element.find_element(By.CLASS_NAME, "JguWG").text.strip()
#
#                     reviews_data.append(review_data)
#                     count += 1
#
#                     if count >= limit:
#                         break
#
#                 except (NoSuchElementException, ElementNotInteractableException) as e:
#                     print(f"Error extracting review data: {e}")
#                     continue  # Skip to the next review if there's an error
#
#             if count >= limit:
#                 break
#
#             try:
#                 # Pagination Logic
#                 next_button_divs = sb.find_elements(By.CLASS_NAME, "xkSty")
#                 next_button = next_button_divs[0].find_element(By.XPATH, ".//a[@aria-label='Next page']")
#
#                 # Check if the button is enabled before clicking
#                 if next_button.is_enabled():
#                     next_button.click()  # Click the next button
#                     time.sleep(random.uniform(3, 6))  # Add a delay to prevent detection
#                 else:
#                     print("Next button is disabled or not clickable.")
#                     break
#
#             except NoSuchElementException:
#                 print("Next button not found.")
#                 break
#
#     return reviews_data

def extract_reviews(url, rating=1, limit=100):
    reviews_data = []
    count = 0
    page_number = 1  # Track the current page number

    filter_button = 'span:contains("Filters")'
    rating_button = f".qgcDG > div:nth-child(2) > div > button:nth-of-type({rating})"
    apply_button = 'span:contains("Apply")'

    with SB(uc=True, demo=True, incognito=True, locale_code="en") as sb:
        sb.uc_open_with_reconnect(url, 4)
        sb.uc_click(filter_button, reconnect_time=2)
        sb.uc_click(rating_button, reconnect_time=2)
        sb.uc_click(apply_button, reconnect_time=2)
        time.sleep(random.uniform(3, 6))

        while count < limit:
            get_reviews = sb.find_element(By.CLASS_NAME, "LbPSX")
            get_each_review_child = get_reviews.find_elements(By.CLASS_NAME, "_c")

            for element in get_each_review_child:
                review_data = {}

                try:
                    review_data['rating_score'] = rating
                    review_link = element.find_element(By.CSS_SELECTOR, ".biGQs._P.fiohW.qWPrE.ncFvv.fOtGX")
                    review_data['review_link_href'] = review_link.find_element(By.XPATH, "./*").get_attribute("href")
                    review_data['review_title'] = review_link.find_element(By.XPATH, "./*").text
                except NoSuchElementException:
                    review_data['review_link_href'] = "N/A"
                    review_data['review_title'] = "N/A"

                # Extract review_id from the review URL
                try:
                    review_id_match = re.search(r"-r(\d+)-", review_data.get('review_link_href', ''))
                    review_data['review_id'] = review_id_match.group(1) if review_id_match else "N/A"
                except Exception:
                    review_data['review_id'] = "N/A"

                # try:
                #     ratings = element.find_element(By.CSS_SELECTOR, ".UctUV.d.H0")
                #     review_data['ratings_score'] = ratings.find_element(By.XPATH, "./*").text.split(" ")[0].strip()
                # except NoSuchElementException:
                #     review_data['ratings_score'] = "N/A"

                try:
                    review_data['review_date'] = element.find_element(By.CLASS_NAME, "RpeCd").text.strip().split('•')[
                        0].strip()
                except NoSuchElementException:
                    review_data['review_date'] = "N/A"

                try:
                    username_entities = element.find_element(By.CSS_SELECTOR, ".biGQs._P.fiohW.fOtGX")
                    review_data['username'] = username_entities.text
                    review_data['username_link'] = username_entities.find_element(By.CSS_SELECTOR,
                                                                                  ".BMQDV._F.Gv.wSSLS.SwZTJ.FGwzt.ukgoS").get_attribute(
                        "href")
                except NoSuchElementException:
                    review_data['username'] = "N/A"
                    review_data['username_link'] = "N/A"

                try:
                    review_data['review_body'] = element.find_element(By.CLASS_NAME, "JguWG").text.strip()
                except NoSuchElementException:
                    review_data['review_body'] = "N/A"

                reviews_data.append(review_data)
                count += 1

                if count >= limit:
                    break

            if count >= limit:
                break

            try:
                # Pagination Logic
                next_button_divs = sb.find_elements(By.CLASS_NAME, "xkSty")
                next_button = next_button_divs[0].find_element(By.XPATH, ".//a[@aria-label='Next page']")

                # Check if the button is enabled before clicking
                if next_button.is_enabled():
                    next_button.click()  # Click the next button
                    time.sleep(random.uniform(3, 6))  # Add a delay to prevent detection
                else:
                    print("Next button is disabled or not clickable.")
                    break

            except NoSuchElementException:
                print("Next button not found.")
                break

    return reviews_data

# Usage
link = input("Enter the attraction URL\n")
ratings_to_extract = [1, 2, 3, 4, 5]
all_reviews = []
location_name = extract_location_name(link)  # Extract location name from URL
location_id = extract_location_id(link)
for rating in ratings_to_extract:
    try:
        reviews = extract_reviews(link, rating, limit=400)  # Extract reviews with a specified limit for each rating
        all_reviews.extend(reviews)  # Add the extracted reviews to the main list
    except Exception as e:
        print(f"Error extracting reviews for rating {rating}: {e}")

# Save all reviews to a single CSV regardless of errors
save_reviews_to_csv(all_reviews,location_name,location_id)  # Save all reviews to a single CSV
