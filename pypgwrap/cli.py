__author__ = 'Erick Almeida'

import code
import connection
        
if __name__ == '__main__':
    db = connection()
    code.interact(local=locals())