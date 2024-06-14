from unidecode import unidecode

# For create py env 
# Step 2: Create a virtual environment
# python3 -m venv myenv

# Step 3: Activate the virtual environment
# source myenv/bin/activate

# hàm bổ trợ xử lý DF

def convert_unidecode(text):
    error_list = []
    try:
        return unidecode(text).lower()
    except Exception as e:
        error_list.append(text)
    
def modified_string(string):
    import re
    modified_string = re.sub(r'0+', ',', string)
    return modified_string

def count_str(text):
    if 'kw' in text:
        from collections import Counter
        text = text.replace(',' , ' ')
        text = text.replace('_' , ' ')
        text_to_list = text.split()
        text_to_dict = dict(Counter(text_to_list))

        return int(text_to_dict['kw'])
    else:
        return 0
    
