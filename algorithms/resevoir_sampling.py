import random
import ConfigParser

total_number_of_sessions = 17991844
print_info = 100000

def create_samples():

    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    sample_count = int(config.get('Config','num_of_sessions'))
    sample_list = []

    # source: https://en.wikipedia.org/wiki/Reservoir_sampling
    if int(config.get('Config','random_sampling')) == 1:
        # Force the value of the seed so the results are repeatable
        random.seed(12345)

        for index in range(total_number_of_sessions):
            # Generate the reservoir
            if index < sample_count:
                sample_list.append(index)
            else:
                # Randomly replace elements in the reservoir
                # with a decreasing probability.
                # Choose an integer between 0 and index (inclusive)
                r = random.randint(0, index)
                if r < sample_count:
                    sample_list[r] = index
                # if index%print_info == 0:
                #     print index
        sample_list.sort()
    else:
        for index in range(sample_count):
            sample_list.append(index)

    return sample_list