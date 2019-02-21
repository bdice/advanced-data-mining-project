#!/usr/bin/env python
import signac

if __name__ == '__main__':
    try:
        project = signac.get_project()
    except LookupError:
        project = signac.init_project('US_Flights')

    for year in range(1993, 2019):
        for quarter in range(1, 5):
            if year == 2018 and quarter == 4:
                continue
            job = project.open_job({'year': year, 'quarter': quarter})
            job.init()
            print('Created {} Q{}'.format(year, quarter))
