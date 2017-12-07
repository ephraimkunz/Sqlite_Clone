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
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

/*
Internal Node Header layout
*/
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;


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

/*
    Internal Node Body Layout
*/
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;

const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

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
void indent(uint32_t level);
void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level);
Cursor* table_find(Table* table, uint32_t key);
Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key);
NodeType get_node_type(void* node);
void set_node_type(void* node, NodeType type);
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value);
uint32_t get_unused_page_num(Pager* pager);
void create_new_root(Table* table, uint32_t right_child_page_num);
uint32_t* internal_node_num_keys(void* node);
uint32_t* internal_node_right_child (void* node);
uint32_t* internal_node_cell(void* node, uint32_t cell_num);
uint32_t* internal_node_child(void* node, uint32_t child_num);
uint32_t* internal_node_key(void* node, uint32_t key_num);
uint32_t get_node_max_key(void* node);
bool is_node_root(void* node);
uint32_t* leaf_node_next_leaf(void* node);
void set_node_root(void* node, bool is_root);
void initialize_internal_node(void* node);
Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key);
uint32_t* node_parent(void* node);
void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key);
uint32_t internal_node_find_child(void* node, uint32_t key);
void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num);

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

void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {
    // Add a new child/key pair to parent that corresponds to child.
    void* parent = get_page(table->pager, parent_page_num);
    void* child = get_page(table->pager, child_page_num);
    uint32_t child_max_key = get_node_max_key(child);
    uint32_t index = internal_node_find_child(parent, child_max_key);

    uint32_t original_num_keys = *internal_node_num_keys(parent);
    *internal_node_num_keys(parent) = original_num_keys + 1;

    if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
        printf("Need to implement splitting internal node\n");
        exit(1);
    }

    uint32_t right_child_page_num = *internal_node_right_child(parent);
    void* right_child = get_page(table->pager, right_child_page_num);

    if (child_max_key > get_node_max_key(right_child)) {
        // Replace right child
        *internal_node_child(parent, original_num_keys) = right_child_page_num;
        *internal_node_key(parent, original_num_keys) = get_node_max_key(right_child);
        *internal_node_right_child(parent) = child_page_num;
    } else {
        // Make room for the new cell
        for (uint32_t i = original_num_keys; i > index; i--) {
            void* dest = internal_node_cell(parent, i);
            void* src = internal_node_cell(parent, i - 1);
            memcpy(dest, src, INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_child(parent, index) = child_page_num;
        *internal_node_key(parent, index) = child_max_key;
    }
}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
}

uint32_t* node_parent(void* node) {
    return node + PARENT_POINTER_OFFSET;
}

uint32_t* leaf_node_next_leaf(void* node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

uint32_t internal_node_find_child(void* node, uint32_t key) {
    /*
    Return the index of the child which should contain the given key.
    */
    uint32_t num_keys = *internal_node_num_keys(node);

    uint32_t min_index = 0;
    uint32_t max_index = num_keys; // There is one more child than key

    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);
        if (key_to_right >= key) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    return min_index;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t num_keys = *internal_node_num_keys(node);

    uint32_t child_index = internal_node_find_child(node, key);
    uint32_t child_num = *internal_node_child(node, child_index);
    void* child = get_page(table->pager, child_num);
    switch (get_node_type(child)) {
    case NODE_LEAF:
        return leaf_node_find(table, child_num, key);
    case NODE_INTERNAL:
        return internal_node_find(table, child_num, key);
    }
}

void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; ++i) {
        printf(" ");
    }
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
    void* node = get_page(pager, page_num);
    uint32_t num_keys, child;

    switch (get_node_type(node)) {
    case NODE_LEAF:
        num_keys = *leaf_node_num_cells(node);
        indent(indentation_level);
        printf("- leaf (size %d)\n", num_keys);
        for (uint32_t i = 0; i < num_keys; ++i) {
            indent(indentation_level + 1);
            printf("- %d\n", *leaf_node_key(node, i));
        }
        break;
    case NODE_INTERNAL:
        num_keys = *internal_node_num_keys(node);
        indent(indentation_level);
        printf("- internal (size %d)\n", num_keys);
        for (uint32_t i = 0; i < num_keys; ++i) {
            child = *internal_node_child(node, i);
            print_tree(pager, child, indentation_level + 1);

            indent(indentation_level);
            printf("- key %d\n", *internal_node_key(node, i));
        }
        child = *internal_node_right_child(node);
        print_tree(pager, child, indentation_level + 1);
        break;
    }
}

void initialize_internal_node(void* node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
}

bool is_node_root(void* node) {
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

void set_node_root(void* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

uint32_t get_node_max_key(void* node) {
    switch (get_node_type(node)) {
        case NODE_INTERNAL:
            return *internal_node_key(node, *internal_node_num_keys(node) - 1);
        case NODE_LEAF:
            return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    }
}

// Not recycling free pages yet.
uint32_t get_unused_page_num(Pager* pager) {
    return pager->num_pages;
}

uint32_t* internal_node_num_keys(void* node) {
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internal_node_right_child (void* node) {
    return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
    return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys) {
        printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
        exit(1);
    } else if (child_num == num_keys) {
        return internal_node_right_child(node);
    } else {
        return internal_node_cell(node, child_num);
    }
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
    return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

void create_new_root(Table* table, uint32_t right_child_page_num) {
    /*
    Handle splitting the root.
    Old root copied to new page, becomes left child.
    Address of right child passed in.
    Re-initialize root page to contain the new root node.
    New root node point to two children.
    */

    void* root = get_page(table->pager, table->root_page_num);
    void* right_child = get_page(table->pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void* left_child = get_page(table->pager, left_child_page_num);

    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);

    /* Root node is a new internal node with one key and two children */
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
    *node_parent(left_child) = table->root_page_num;
    *node_parent(right_child) = table->root_page_num;
}

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    /*
    Create a new node and move half the cells over.
    Insert the new value into one of the two nodes.
    Update parent or create new parent.
    */

    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t old_max = get_node_max_key(old_node);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    *node_parent(new_node) = *node_parent(old_node);
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node) = new_page_num;

    /*
    All existing keys plus new key should be divided evenly between
    old (left) and new (right) nodes.
    Starting from the right, move each key to correct position.
    */
    for(int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
        void* destination_node = (i >= LEAF_NODE_LEFT_SPLIT_COUNT) ? new_node : old_node;
        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = leaf_node_cell(destination_node, index_within_node);

        if (i == cursor->cell_num) {
            serialize_row(value, leaf_node_value(destination_node, index_within_node));
            *leaf_node_key(destination_node, index_within_node) = key;
        } else if (i > cursor->cell_num) {
            memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        }
    }


    /* Update cell count on both leaf nodes */
    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if (is_node_root(old_node)) {
        return create_new_root(cursor->table, new_page_num);
    } else {
        uint32_t parent_page_num = *node_parent(old_node);
        uint32_t new_max = get_node_max_key(old_node);
        void* parent = get_page(cursor->table->pager, parent_page_num);

        update_internal_node_key(parent, old_max, new_max);
        internal_node_insert(cursor->table, parent_page_num, new_page_num);
        return;
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
        return internal_node_find(table, root_page_num, key);
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
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;
}

 void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if(num_cells >= LEAF_NODE_MAX_CELLS) {
        // Node full
        leaf_node_split_and_insert(cursor, key, value);
        return;
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
        print_tree(table->pager, 0, 0);
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
    Cursor* cursor = table_find(table, 0);

    void* node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
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
        /* Advance to next leaf node */
        uint32_t next_page_num = *leaf_node_next_leaf(node);
        if (next_page_num == 0) { // Rightmost leaf
            cursor->end_of_table = true;
        } else {
            cursor->page_num = next_page_num;
            cursor->cell_num = 0;
        }
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
        set_node_root(root_node, true);
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
