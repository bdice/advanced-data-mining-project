import signac
import os

pr = signac.get_project()

for job in pr:
    expected_size = job.doc['Coupon']['file_size']
    if job.isfile('Coupon.csv'):
        actual_size = os.stat(job.fn('Coupon.csv')).st_size
    else:
        actual_size = 0
    if expected_size != actual_size:
        print(job._id)
