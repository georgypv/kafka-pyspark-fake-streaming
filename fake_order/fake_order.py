import random
from faker.providers import BaseProvider

class OrderProvider(BaseProvider):

    def book_category(self):
        categories = ['Fiction',
                      'Poetry',
                      'Non-Fiction',
                      'Recipes',
                      'Technical Literature',
                      'Dictionaries',
                      'Other',
                      ]
        return categories[random.randint(0, len(categories) - 1)]

    def book_format(self):
        book_format = ['E-Book', 'Paperback', 'Hardcover', 'Used',]
        return book_format[random.randint(0, len(book_format) - 1)]

    def book_rating(self):
        return random.randint(0, 10)