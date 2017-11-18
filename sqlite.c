#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t COLUMN_EMAIL_SIZE = 255;
const uint32_t COLUMN_USERNAME_SIZE = 32;
const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100;

typedef enum meta_command_result_t {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED
} MetaCommandResult;

typedef enum execute_result_t {
    EXECUTE_TABLE_FULL,
    EXECUTE_SUCCESS
} ExecuteResult;

typedef enum prepare_result_t {
    PREPARE_SUCCESS,
    PREPARE_INVALID_ID,
    PREPARE_UNRECOGNIZED,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG
} PrepareResult;

typedef enum prepared_type_t {
    STATEMENT_INSERT, STATEMENT_SELECT
} StatementType;

typedef struct input_buffer_t {
    char* buffer;
    size_t buffer_len;
    ssize_t input_len;
} InputBuffer;

typedef struct table_t {
    void* pages[TABLE_MAX_PAGES];
    uint32_t num_rows;
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

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

void print_prompt();
void read_input(InputBuffer* buffer);
MetaCommandResult do_meta_command(InputBuffer* buffer);
InputBuffer* new_input_buffer();
PrepareResult prepare_statement(InputBuffer* buffer, Statement* statement);
ExecuteResult execute_statement(Statement* statement, Table* table);
void serialize_row(Row* source, void* destination);
void deserialize_row(void* source, Row* destination);
void* row_slot(Table* table, uint32_t row_num);
ExecuteResult execute_select(Statement* statement, Table* table);
ExecuteResult execute_insert(Statement* statement, Table* table);
Table* new_table();
void print_row(Row* row);

int main() {
    Table* table = new_table();
    InputBuffer* input_buffer = new_input_buffer();

    while(true) {
        print_prompt();
        read_input(input_buffer);

        if(input_buffer->buffer[0] == '.') { // Meta command
            switch(do_meta_command(input_buffer)) {
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
        }
    }
}

MetaCommandResult do_meta_command(InputBuffer* buffer) {
    if(strcmp(buffer->buffer, ".exit") == 0) {
        exit(0);
    } else {
        return META_COMMAND_UNRECOGNIZED;
    }
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
    if(table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Row* row_to_insert = &(statement->row_to_insert);
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows++;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Row row;
    for(uint32_t i = 0; i < table->num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }

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

void* row_slot(Table* table, uint32_t row_num) {
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = table->pages[page_num];
    if(!page) { // No page yet, so allocate memory here (when we try to access it)
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE; // Which row index in page
    uint32_t byte_offset = row_offset * ROW_SIZE; // Which byte in the page does this actually map to
    return page + byte_offset;
}

void print_prompt() {
    printf("db > ");
}

Table* new_table() {
    Table* table = (Table*)malloc(sizeof(Table));
    table->num_rows = 0;
    return table;
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
