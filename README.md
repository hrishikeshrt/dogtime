# Dogs Database

Comprehensive information about more than 375 breeds of dogs, their vital statistics and their traits, scraped form https://dogtime.com

## Usage

* **Scrape**

`python dogtime.py` will fetch complete database and prepare a table of breeds, their vital statistics and their traits.

* **Use in a project**

```python

from dogtime import DogTime

D = DogTime(data_dir='<your-data-dir>')
breeds = D.get_all_breeds()
for breed in breeds:
    D.get_breed_details(breed['id'])

data, df = D.prepare_table()
```

## TODO

* Documentation
* Cleaning data in introduction, description etc.
* Add search capabilities for breeds, traits etc.
* Add NLP for question answering.