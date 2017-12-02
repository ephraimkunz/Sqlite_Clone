#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/errno.h>
#include <unistd.h>

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t COLUMN_EMAIL_SIZE = 255;
const uint32_t COLUMN_USERNAME_SIZE = 32;
const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100;

/*
    Common Node Header layout
*/
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = IS_ROOT_SIZE + NODE_TYPE_SIZE + PARENT_POINTER_SIZE;

/*
    Leaf Node Header layout
*/
// Cell is key/value pair
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

typedef enum meta_command_result_t {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED
} MetaCommandResult;

typedef enum execute_result_t {
    EXECUTE_TABLE_FULL,
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY
} ExecuteResult;

typedef enum prepare_result_t {
    PREPARE_SUCCESS,
    PREPARE_INVALID_ID,
    PREPARE_UNRECOGNIZED,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG
} PrepareResult;

typedef enum node_type_t {
    NODE_INTERNAL, NODE_LEAF
} NodeType;

typedef enum prepared_type_t {
    STATEMENT_INSERT, STATEMENT_SELECT
} StatementType;

typedef struct input_buffer_t {
    char* buffer;
    size_t buffer_len;
    ssize_t input_len;
} InputBuffer;

typedef struct pager_t {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct table_t {
    Pager* pager;
    uint32_t root_page_num;
} Table;

typedef struct row_t {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct statement_t {
    StatementType type;
    Row row_to_insert;
} Statement;

typedef struct cursor_t {
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table; // Indicates a position one past the last element.
} Cursor;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

/*
    Leaf Node Body layout
*/
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

void print_prompt();
void read_input(InputBuffer* buffer);
MetaCommandResult do_meta_command(InputBuffer* buffer, Table* table);
InputBuffer* new_input_buffer();
PrepareResult prepare_statement(InputBuffer* buffer, Statement* statement);
ExecuteResult execute_statement(Statement* statement, Table* table);
void serialize_row(Row* source, void* destination);
void deserialize_row(void* source, Row* destination);
void* cursor_value(Cursor* cursor);
ExecuteResult execute_select(Statement* statement, Table* table);
ExecuteResult execute_insert(Statement* statement, Table* table);
Table* db_open(const char* filename);
void print_row(Row* row);
Pager* pager_open(const char* filename);
void* get_page(Pager* pager, uint32_t page_num);
void db_close(Table* table);
void pager_flush(Pager* pager, uint32_t page_num);
Cursor* table_start(Table* table);
void cursor_advance(Cursor* cursor);
uint32_t* leaf_node_num_cells(void* node);
void* leaf_node_cell(void*node, uint32_t cell_num);
uint32_t* leaf_node_key(void* node, uint32_t cell_num);
void* leaf_node_value(void* node, uint32_t cell_num);
void initialize_leaf_node(void* node);
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value);
void print_constants();
void print_leaf_node(void* node);
Cursor* table_find(Table* table, uint32_t key);
Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key);
NodeType get_node_type(void* node);
void set_node_type(void* node, NodeType type);

int main(int argc, char* argv[]) {
    if(argc < 2) {
        printf("Must supply a database filename\n");
        exit(1);
    }

    Table* table = db_open(argv[1]);
    InputBuffer* input_buffer = new_input_buffer();

    while(true) {
        print_prompt();
        read_input(input_buffer);

        if(input_buffer->buffer[0] == '.') { // Meta command
            switch(do_meta_command(input_buffer, table)) {
            case (META_COMMAND_SUCCESS):
                continue;
            case META_COMMAND_UNRECOGNIZED:
                printf("Unrecognized command [%s]\n", input_buffer->buffer);
                continue;
            }
        }

        // Not a meta command
        Statement statement;
        switch(prepare_statement(input_buffer, &statement)) {
        case PREPARE_SUCCESS:
            break;
        case PREPARE_SYNTAX_ERROR:
            printf("Syntax error. Could not parse statement\n");
            continue;
        case PREPARE_UNRECOGNIZED:
            printf("Unrecognized keyword at start of [%s]\n", input_buffer->buffer);
            continue;
        case PREPARE_STRING_TOO_LONG:
            printf("String is too long\n");
            continue;
        case PREPARE_INVALID_ID:
            printf("Id must be positive\n");
            continue;
        }

        // We have a real Statement here
        switch(execute_statement(&statement, table)) {
        case EXECUTE_SUCCESS:
            printf("Executed\n");
            break;
        case EXECUTE_TABLE_FULL:
            printf("Error: Table full\n");
            break;
        case EXECUTE_DUPLICATE_KEY:
            printf("Error: Duplicate key\n");
            break;
        }
    }
}

NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *(uint8_t*)(node + NODE_TYPE_OFFSET) = value;
}

/*
    Return position of given key. If not present, return position where it should be inserted.
*/
Cursor* table_find(Table* table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    void* root_node = get_page(table->pager, root_page_num);

    if(get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, root_page_num, key);
    } else {
        printf("Need to implement searching an internal node\n");
        exit(1);
    }
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;

    // Binary search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while(one_past_max_index != min_index) {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);
        if(key == key_at_index) {
            cursor->cell_num = index;
            return cursor;
        }
        if(key < key_at_index) {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    cursor->cell_num = min_index;
    return cursor;
}

uint32_t* leaf_node_num_cells(void* node) {
    return (uint32_t*)((char*)node + LEAF_NODE_NUM_CELLS_OFFSET);
}

void print_leaf_node(void* node) {
    uint32_t num_cells = *leaf_node_num_cells(node);
    printf("leaf (size %d)\n", num_cells);
    for(uint32_t i = 0; i < num_cells; ++i) {
        uint32_t key = *leaf_node_key(node, i);
        printf("  - %d : %d\n", i, key);
    }
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
    return (char*)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void* node) {
    set_node_type(node, NODE_LEAF);
    *leaf_node_num_cells(node) = 0;
}

 void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if(num_cells >= LEAF_NODE_MAX_CELLS) {
        // Node full
        printf("Need to implement splitting a leaf node\n");
        exit(1);
    }

    if(cursor->cell_num < num_cells) {
        // Make room for a new cell
        for(uint32_t i = num_cells; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
 }

MetaCommandResult do_meta_command(InputBuffer* buffer, Table* table) {
    if(strcmp(buffer->buffer, ".exit") == 0) {
        db_close(table);
        exit(0);
    } else if(strcmp(buffer->buffer, ".constants") == 0) {
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
    } else if(strcmp(buffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_leaf_node(get_page(table->pager, 0));
        return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED;
    }
}

void print_constants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

Cursor* table_start(Table* table) {
    Cursor* cursor = (Cursor*) malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;

    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

PrepareResult prepare_insert(InputBuffer* buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;

    char* keyword = strtok(buffer->buffer, " ");
    char* id_str = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if(!id_str || !username || !email) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_str);
    if(id < 0)
        return PREPARE_INVALID_ID;

    if(strlen(username) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE)
        return PREPARE_STRING_TOO_LONG;

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

Pager* pager_open(const char* filename) {
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

    if(fd == -1) {
        printf("Unable to open file\n");
        exit(1);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager* pager = (Pager*) malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = file_length / PAGE_SIZE;

    if(file_length % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupt file\n");
        exit(1);
    }

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void db_close(Table* table) {
    Pager* pager = table->pager;

    for(uint32_t i = 0; i < pager->num_pages; ++i) {
        if(pager->pages[i] == NULL) {
            continue;
        }

        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int result = close(pager->file_descriptor);
    if(result == -1) {
        printf("Error closing db file\n");
        exit(1);
    }

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        void* page = pager->pages[i];
        if(page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
}

void pager_flush(Pager* pager, uint32_t page_num) {
    if(pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(1);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if(offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(1);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

    if(bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(1);
    }
}

PrepareResult prepare_statement(InputBuffer* buffer, Statement* statement) {
    if(strncmp(buffer->buffer, "insert", 6) == 0) { // Starts with 'insert'...followed by more
       return prepare_insert(buffer, statement);
    }

    if(strcmp(buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED;
}

// This is our VM
ExecuteResult execute_statement(Statement* statement, Table* table) {
    switch(statement->type) {
    case STATEMENT_INSERT:
        return execute_insert(statement, table);
    case STATEMENT_SELECT:
        return execute_select(statement, table);
    }
}

ExecuteResult execute_insert(Statement* statement, Table* table){
    void* node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    if(num_cells >= LEAF_NODE_MAX_CELLS) {
        return EXECUTE_TABLE_FULL;
    }

    Row* row_to_insert = &(statement->row_to_insert);
    uint32_t key_to_insert = row_to_insert->id;
    Cursor* cursor = table_find(table, key_to_insert);

    if(cursor->cell_num < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
        if(key_at_index == key_to_insert) {
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

    free(cursor);

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Cursor* cursor = table_start(table);
    Row row;

    while(!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }

    free(cursor);

    return EXECUTE_SUCCESS;
}

void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row* source, void* destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);    
}

void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* cursor_value(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);

    cursor->cell_num ++;
    if(cursor->cell_num >= (*leaf_node_num_cells(node))) {
        cursor->end_of_table = true;
    }
}

void print_prompt() {
    printf("db > ");
}

Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);

    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;

    if(pager->num_pages == 0) {
        // New db file. Initialize page 0 as leaf node
        void* root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
    }

    return table;
}

void* get_page(Pager* pager, uint32_t page_num) {
    if(page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(1);
    }

    if(pager->pages[page_num] == NULL) {
        // Cache miss. Allocate memory and load from file.
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        // We might save a partial page at the end of the file
        if( pager->file_length  % PAGE_SIZE) {
            num_pages ++;
        }

        if(page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if(bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(1);
            }
        }

        pager->pages[page_num] = page;

        if(page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
    }

    return pager->pages[page_num];
}

void read_input(InputBuffer* buffer) {
    buffer->input_len = getline(&buffer->buffer, &buffer->buffer_len, stdin);
    if(buffer->input_len < 0) {
        printf("Error reading input\n");
        exit(1);
    }

    // Don't keep terminating newline
    buffer->input_len --;
    buffer->buffer[buffer->input_len] = 0;
}

InputBuffer* new_input_buffer() {
    InputBuffer* in = (InputBuffer*)malloc(sizeof(InputBuffer));
    in->buffer = NULL;
    in->buffer_len = 0;
    in->input_len = 0;
    return in;
}
