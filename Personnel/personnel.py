# from Personnel.cleaner import Cleaner
#
#
# class Personnel:
#     def __init__(self):
#
#         self.personnel = None
#
#     @staticmethod
#     def get_cleaner_data(cleaner_id):
#         cl = Cleaner(cleaner_id=cleaner_id)
#         return cl
#
#     def get_all_personnel(self):
#         """
#         Read from DB all the cleaner ID's
#         """
#         db_cleaner_ids = ['0001', '0002']  # From DB
#
#         out = list(map(self.get_cleaner_data, db_cleaner_ids))
#
#         return out
