#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dogs Database by collecting data from DogTime.com

@author: Hrishikesh Terdalkar
"""

###############################################################################

import re
import os
import json
import logging

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd

###############################################################################

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) '
                   'Gecko/20100101 Firefox/89.0')
}

DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')

###############################################################################

IGNORE = [
    " shelters and rescues", "adopt!", "Don't shop",
    " here ", "here.", "here!", " buy ", "you should also pick up"
    "awesome crate", "Glyde", "DogTime ",
    "take a look at", "See below", "you can find"
]

###############################################################################


class DogTime(requests.Session):
    SERVER = 'dogtime.com'
    SITEMAP = {
        'quiz': 'quiz/dog-breed-selector',
        'breeds': 'dog-breeds/profiles',
        'groups': 'dog-breeds/groups',
        'characteristics': 'dog-breeds/characteristics'
    }
    URL = {
        'dog': 'dog-breeds',
        'breed': 'dog-breeds',
        'group': 'dog-breeds/groups',
        'characteristic': 'dog-breeds/characteristics',
        'trait': 'dog-breeds/characteristics',
        'quality': 'dog-breeds/characteristics'
    }

    def __init__(self, data_dir=DATA_DIR, use_cache=True, *args, **kwargs):
        self.data_dir = data_dir
        self.use_cache = use_cache
        self.groups_dir = os.path.join(self.data_dir, 'groups')
        self.traits_dir = os.path.join(self.data_dir, 'traits')
        self.breeds_dir = os.path.join(self.data_dir, 'breeds')

        for required_dir in [
            self.data_dir,
            self.groups_dir,
            self.traits_dir,
            self.breeds_dir
        ]:
            os.makedirs(required_dir, exist_ok=True)

        super().__init__(*args, **kwargs)
        self.headers.update(HEADERS)
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_url(self, url_key):
        if self.SITEMAP.get(url_key):
            return f"https://{self.SERVER}/{self.SITEMAP[url_key]}"

    def build_url(self, item_type, item_id):
        if item_type in self.URL:
            return f"https://{self.SERVER}/{self.URL[item_type]}/{item_id}"

    def extract_id(self, url_or_id):
        if '/' not in url_or_id:
            return url_or_id

        pattern = (
            rf'^(http://|https://|)(www.|)({self.SERVER}|)'
            r'/dog-breeds/(groups/|characteristics/|)([^/]+)$'
        )
        m = re.match(pattern, url_or_id)
        if m:
            url_id = m.group(5)
            return url_id

    def get_all_groups(self):
        groups_file = os.path.join(self.data_dir, 'groups.json')
        if os.path.isfile(groups_file) and self.use_cache:
            with open(groups_file) as f:
                groups = json.load(f)
            return groups

        url = self.get_url('groups')
        content = self.get(url).content.decode()
        soup = BeautifulSoup(content, 'html.parser')
        list_elements = soup.find_all('li', class_='item paws')
        groups = []

        for list_element in list_elements:
            info_div = list_element.find('div', class_='info-archive')
            group_name = info_div.find('h3').get_text().strip()
            group_url = info_div.find('a')['href']
            group_excerpt = info_div.find(
                'div', class_='callout-excerpt'
            ).get_text().strip()

            group = {
                'id': self.extract_id(group_url),
                'name': group_name,
                'url': group_url,
                'excerpt': group_excerpt
            }
            groups.append(group)

        with open(groups_file, 'w') as f:
            json.dump(groups, f, indent=2)

        return groups

    def get_group_details(self, group_url_or_id):
        group_id = self.extract_id(group_url_or_id)

        group_file = os.path.join(self.groups_dir, f'{group_id}.json')
        if os.path.isfile(group_file) and self.use_cache:
            with open(group_file) as f:
                group = json.load(f)
            return group

        group_url = self.build_url('group', group_id)
        content = self.get(group_url).content.decode()
        soup = BeautifulSoup(content, 'html.parser')
        article_div = soup.find('div', class_='category-article-main')

        group_name = article_div.find('h1').get_text().strip()

        paragraphs = article_div.find_all('p', class_=None)
        text = '\n'.join([p.get_text() for p in paragraphs])

        ul = soup.find('ul', class_='breed-thumbnail-column-list')
        breed_links = ul.find_all('a', class_='list-item-img')
        members = [self.extract_id(a['href']) for a in breed_links]

        group = {
            'id': self.extract_id(group_url),
            'name': group_name,
            'url': group_url,
            'description': text,
            'members': members
        }

        with open(group_file, 'w') as f:
            json.dump(group, f, indent=2)
        return group

    def get_all_traits(self):
        traits_file = os.path.join(self.data_dir, 'traits.json')
        if os.path.isfile(traits_file) and self.use_cache:
            with open(traits_file) as f:
                traits = json.load(f)
            return traits

        url = self.get_url('characteristics')
        content = self.get(url).content.decode()
        soup = BeautifulSoup(content, 'html.parser')
        list_elements = soup.find_all('li', class_='item paws')
        traits = []

        for list_element in list_elements:
            info_div = list_element.find('div', class_='info-archive')
            trait_url = info_div.find('a')['href']
            trait_name = info_div.find('h3').get_text().strip()
            trait_excerpt = info_div.find(
                'div', class_='callout-excerpt'
            ).get_text().strip()
            traits.append({
                'id': self.extract_id(trait_url),
                'name': trait_name,
                'url': trait_url,
                'excerpt': trait_excerpt
            })

        with open(traits_file, 'w') as f:
            json.dump(traits, f, indent=2)

        return traits

    def get_trait_details(self, trait_url_or_id):
        trait_id = self.extract_id(trait_url_or_id)

        trait_file = os.path.join(self.traits_dir, f'{trait_id}.json')
        if os.path.isfile(trait_file) and self.use_cache:
            with open(trait_file) as f:
                trait = json.load(f)
            return trait

        trait_url = self.build_url('trait', trait_id)
        content = self.get(trait_url).content.decode()
        soup = BeautifulSoup(content, 'html.parser')

        article_div = soup.find('div', class_='category-article-main')
        trait_name = article_div.find('h1').get_text().strip()
        paragraphs = article_div.find_all('p', class_=None)
        description = '\n'.join([
            p.get_text()
            for p in paragraphs
        ]).replace(u'\xa0', u' ')

        excerpt_div = article_div.find('div', class_='callout-excerpt')
        related_traits = [
            self.extract_id(a['href'])
            for a in excerpt_div.find_all('a')
            if '/dog-breeds/characteristics/' in a['href']
        ]

        m_div = article_div.find('div', class_='breed-categories-container')
        short_description = m_div.find('h3').get_text().strip()

        m_ul = soup.find('ul', class_='breed-categories-wrapper')
        breed_links = m_ul.find_all('a', class_='column-link')
        members = [self.extract_id(a['href']) for a in breed_links]

        trait = {
            'id': trait_id,
            'url': trait_url,
            'name': trait_name,
            'short': short_description,
            'description': description,
            'related-traits': related_traits,
            'members': members
        }
        with open(trait_file, 'w') as f:
            json.dump(trait, f, indent=2)

        for related_trait in trait['related-traits']:
            self.get_trait_details(related_trait)

        return trait

    def get_all_breeds(self):
        breeds_file = os.path.join(self.data_dir, 'breeds.json')
        if os.path.isfile(breeds_file) and self.use_cache:
            with open(breeds_file) as f:
                breeds = json.load(f)
            return breeds

        url = self.get_url('breeds')
        content = self.get(url).content.decode()
        soup = BeautifulSoup(content, 'html.parser')
        breeds = []
        for breed in soup.find_all('a', class_='list-item-img'):
            breed_url = breed['href']
            _breed_img = breed.find('img', class_='list-item-breed-img')
            breed_name = _breed_img['alt']
            breed_image = _breed_img['src']
            breeds.append({
                'id': self.extract_id(breed_url),
                'name': breed_name,
                'url': breed_url,
                'image': breed_image
            })

        with open(breeds_file, 'w') as f:
            json.dump(breeds, f, indent=2)

        return breeds

    def get_breed_details(self, breed_url_or_id):
        breed_id = self.extract_id(breed_url_or_id)

        breed_file = os.path.join(self.breeds_dir, f'{breed_id}.json')
        if os.path.isfile(breed_file) and self.use_cache:
            with open(breed_file) as f:
                breed = json.load(f)
            return breed

        breed_url = self.build_url('breed', breed_id)
        html = self.get(breed_url).content.decode()
        soup = BeautifulSoup(html, 'html.parser')
        breed_div = soup.find('div', class_='breeds-single-content')
        breed_name = breed_div.find('h1').get_text().strip()
        paragraphs = breed_div.find(
            'div', class_='breeds-single-intro'
        ).find_all('p', class_=None)

        intro = '\n'.join([
            p.get_text()
            for p in paragraphs
        ]).replace(u'\xa0', u' ')

        trait_divs = soup.find_all(
            'div', class_='breed-characteristics-ratings-wrapper'
        )
        traits = []
        for trait_div in trait_divs:
            parent_div = trait_div.find('div', class_='parent-characteristic')
            parent_name = parent_div.find('h3').get_text().strip()
            parent_stars_div = parent_div.find(
                'div', class_='characteristic-star-block'
            ).find('div', class_='star')
            parent_stars = [
                int(c.replace('star-', ''))
                for c in parent_stars_div['class'] if 'star-' in c
            ][0]
            traits.append({
                'name': parent_name,
                'parent': None,
                'stars': parent_stars
            })

            child_divs = trait_div.find_all(
                'div', class_='child-characteristic'
            )
            for child_div in child_divs:
                child_name = child_div.find(
                    'div', class_='characteristic-title'
                ).get_text().strip()
                child_stars_div = child_div.find(
                    'div', class_='characteristic-star-block'
                ).find('div', class_='star')
                child_stars = [
                    int(c.replace('star-', ''))
                    for c in child_stars_div['class'] if 'star-' in c
                ][0]

                traits.append({
                    'name': child_name,
                    'parent': parent_name,
                    'stars': child_stars
                })

        vitals = []
        vitals_div = soup.find('div', class_='breed-vital-stats')
        for vital_stat in vitals_div.find_all('div', class_='vital-stat-box'):
            vital_all = vital_stat.get_text().strip()
            if not vital_all:
                continue

            title_div = vital_stat.find('div', class_='vital-stat-title')
            vital_title = title_div.get_text()

            vital_val_start = vital_all.index(vital_title) + len(vital_title)
            vital_value = vital_all[vital_val_start:].strip()
            vitals.append({
                'name': vital_title.replace(':', '').strip(),
                'value': vital_value
            })

            description = {}
            description_divs = soup.find(
                'ul', class_='profile-descriptions-list'
            ).find_all('li', class_='breed-data-item')

            for description_div in description_divs:
                name = description_div.find(
                    'h3', class_='description-title'
                ).get_text().strip()
                desc = description_div.find(
                    'div', class_='breed-data-item-content'
                ).get_text().strip()
                description[name] = desc.replace(u'\xa0', u' ')

        breed = {
            'id': breed_id,
            'url': breed_url,
            'name': breed_name,
            'intro': intro,
            'description': description,
            'vitals': vitals,
            'traits': traits
        }

        with open(breed_file, 'w') as f:
            json.dump(breed, f, indent=2)

        return breed

    def get_breed_selector_questions(self):
        quiz_file = os.path.join(self.data_dir, 'quiz.json')
        if os.path.isfile(quiz_file) and self.use_cache:
            with open(quiz_file) as f:
                questions = json.load(f)
            return questions

        url = self.get_url('quiz')
        html = self.get(url).content.decode()
        soup = BeautifulSoup(html, 'html.parser')
        question_divs = soup.find_all('div', class_='question')
        questions = []
        for question_div in question_divs:
            question_id = question_div['data-question']
            question_text = question_div.find(
                'div', class_='title'
            ).get_text().strip()

            answer_labels = question_div.find_all('label')
            answers = []
            for answer_label in answer_labels:
                radio = answer_label.find('input', class_='radio-answer')
                form_field_name = radio['name']
                answer_id = radio['value']
                answer_text = answer_label.get_text().strip()
                answers.append({
                    'id': answer_id,
                    'name': form_field_name,
                    'answer': answer_text
                })

            question = {
                'id': question_id,
                'question': question_text,
                'answers': answers
            }
            questions.append(question)

        with open(quiz_file, 'w') as f:
            json.dump(questions, f, indent=2)

        return questions

    def get_all_data(self):
        self.logger.debug("Fetching groups ...")
        groups = self.get_all_groups()
        self.logger.debug(f"Fetched {len(groups)} groups.")
        self.logger.debug("Fetching group details ..")
        for group in tqdm(groups):
            self.get_group_details(group['id'])

        self.logger.debug("Fetching traits ...")
        traits = self.get_all_traits()
        self.logger.debug(f"Fetched {len(traits)} groups.")
        self.logger.debug("Fetching traits details ...")
        for trait in tqdm(traits):
            self.get_trait_details(trait['id'])

        self.logger.debug("Fetching breeds ...")
        breeds = self.get_all_breeds()
        self.logger.debug(f"Fetched {len(breeds)} breeds.")
        self.logger.debug("Fetching breeds details ...")
        for breed in tqdm(breeds):
            self.get_breed_details(breed['id'])

        self.logger.debug("Fetching breed selector quiz ...")
        questions = self.get_breed_selector_questions()
        self.logger.debug(f"Fetched {len(questions)} questions.")

    def prepare_table(self):
        self.get_all_data()

        all_breeds = [breed['id'] for breed in self.get_all_breeds()]
        all_traits = {
            trait['name']: trait['id']
            for trait in self.get_all_traits()
        }
        all_groups = {
            group['name']: group['id']
            for group in self.get_all_groups()
        }

        breeds_data = []
        for breed_id in tqdm(all_breeds):
            breed = self.get_breed_details(breed_id)
            breed_data = {
                'id': breed['id'],
                'name': breed['name'],
            }
            for vital in breed['vitals']:
                vital_id = f"vital-{'-'.join(vital['name'].lower().split())}"
                breed_data[vital_id] = vital['value']
                if vital_id == 'vital-dog-breed-group':
                    if vital['value'] in all_groups:
                        breed_data[vital_id] = all_groups[vital['value']]

            for trait in breed['traits']:
                trait_id = f"trait-{'-'.join(trait['name'].lower().split())}"
                if trait['parent'] is not None:
                    if trait['name'] in all_traits:
                        trait_id = f"trait-{all_traits[trait['name']]}"
                    breed_data[trait_id] = trait['stars']

            breeds_data.append(breed_data)

        df = pd.DataFrame(breeds_data)
        df.to_csv(os.path.join(self.data_dir, 'breed_stats.csv'), index=False)

        return breeds_data, df

###############################################################################


if __name__ == '__main__':
    D = DogTime()
    D.prepare_table()
