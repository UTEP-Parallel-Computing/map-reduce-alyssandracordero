#Alyssandra M. Cordero Assignment 2 Pt.1 Map Reduce
import time
import pymp

files_to_read = ["shakespeare1.txt", "shakespeare2.txt", "shakespeare3.txt", "shakespeare4.txt",
                    "shakespeare5.txt", "shakespeare6.txt", "shakespeare7.txt", "shakespeare8.txt"]

word_list = ["hate", "love", "death", "night", "sleep", "time", "henry", "hamlet", "you", "my", "blood",
             "poison", "macbeth", "king", "heart", "honest"]

# Returns a list with the words found in a document 
def read_files(file_to_read):
    with open(file_to_read, 'r') as file:
        rtn = file.read().split()
    return rtn

# Counts the number of occurrences of a word in a list of words
def get_count(word, words_list):
    count = 0
    for i in words_list:
        if word in i.lower():
            count += 1
    return count

# Returns a list of all words in all docs
def combine_docs(files_to_read):
    words = []
    for i in files_to_read:
        words.extend(read_files(i))
    return words

# PARALLEL - returns the dictionary containing the count of words
def count_words_in_files(threads, word_list, files_to_read):
    #combine all of the docs in a single list of words
    content = combine_docs(files_to_read)
    
    #global dictionary
    shared_dict = pymp.shared.dict()

    # Count the number of times a word appears in the files and store it in the dict
    with pymp.Parallel(threads) as p:
        for word in p.iterate(word_list):
            st = time.time()
            shared_dict[word] = get_count(word, content)
            et = time.time()
            print('Time for counting ',word,' was: ', (et - st))
    return shared_dict

def main():
    results = {
        'hate': 332,
        'love': 3070,
        'death': 1016,
        'night': 1402,
        'sleep': 470,
        'time': 1806,
        'henry': 661,
        'hamlet': 475,
        'you': 23304,
        'my': 14203,
        'blood': 1009,
        'poison': 139,
        'macbeth': 288,
        'king': 4545,
        'heart': 1458,
        'honest': 434
    }

    #print requirements
    for threads in range(1,9):
        start_time = time.time()
        map_reduce = count_words_in_files(threads, word_list, files_to_read)
        end_time = time.time()
        total_time = end_time - start_time
        print('Time for Total Instances of All Words: ',total_time, ' when running with ',threads, 'threads')
    for word,occurrances in map_reduce.items():
        print(f'Total of occurrences for {word} : {occurrances}')

    print('End of the program!')

if __name__ == "__main__":
    main()