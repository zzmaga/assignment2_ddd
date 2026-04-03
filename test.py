import requests
from bs4 import BeautifulSoup
import csv

headers = {
    "User-Agent": "Mozilla/5.0"
}

main_sitemap = "https://www.sulpak.kz/sitemap.xml"

response = requests.get(main_sitemap, headers=headers)
soup = BeautifulSoup(response.text, "xml")

regional_sitemaps = []

for loc in soup.find_all("loc"):
    regional_sitemaps.append(loc.text)

print("Найдено региональных sitemap:", len(regional_sitemaps))

# берем только первые 5 для теста
regional_sitemaps = regional_sitemaps[:5]

product_links = []

for sitemap in regional_sitemaps:

    print("Проверяем:", sitemap)

    r = requests.get(sitemap, headers=headers)
    s = BeautifulSoup(r.text, "xml")

    for loc in s.find_all("loc"):
        url = loc.text

        if "/g/" in url:
            product_links.append(url)

print("Найдено товаров:", len(product_links))

with open("products.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["url"])

    for link in product_links[:50]:
        writer.writerow([link])

print("Сохранено", min(len(product_links), 50))