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

typedef struct pager_t {
    int file_descriptor;
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct table_t {
    Pager* pager;
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

typedef struct cursor_t {
    Table* table;
    uint32_t row_num;
    bool end_of_table; // Indicates a position one past the last element.
} Cursor;

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
void pager_flush(Pager* pager, uint32_t page_num, uint32_t size);
Cursor* table_start(Table* table);
Cursor* table_end(Table* table);
void cursor_advance(Cursor* cursor);

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
        }
    }
}

MetaCommandResult do_meta_command(InputBuffer* buffer, Table* table) {
    if(strcmp(buffer->buffer, ".exit") == 0) {
        db_close(table);
        exit(0);
    } else {
        return META_COMMAND_UNRECOGNIZED;
    }
}

Cursor* table_start(Table* table) {
    Cursor* cursor = (Cursor*) malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = table->num_rows == 0;

    return cursor;
}

Cursor* table_end(Table* table) {
    Cursor* cursor = (Cursor*) malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;

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

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; ++i) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void db_close(Table* table) {
    Pager* pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

    for(uint32_t i = 0; i < num_full_pages; ++i) {
        if(pager->pages[i] == NULL) {
            continue;
        }

        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // There may be a partial page to write to the end of the file. This should not 
    // be needed after we switch to a B-tree.
    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if(num_additional_rows > 0) {
        uint32_t page_num = num_full_pages;
        if(pager->pages[page_num] != NULL) {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
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

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
    if(pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(1);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if(offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(1);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);

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
    if(table->num_rows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }

    Row* row_to_insert = &(statement->row_to_insert);
    Cursor* cursor = table_end(table);
    serialize_row(row_to_insert, cursor_value(cursor));
    table->num_rows++;

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
    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = get_page(cursor->table->pager, page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE; // Which row index in page
    uint32_t byte_offset = row_offset * ROW_SIZE; // Which byte in the page does this actually map to
    return page + byte_offset;
}

void cursor_advance(Cursor* cursor) {
    cursor->row_num ++;
    cursor->end_of_table = cursor->row_num >= cursor->table->num_rows;
}

void print_prompt() {
    printf("db > ");
}

Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;

    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;
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
