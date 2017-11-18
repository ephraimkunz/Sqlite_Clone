#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

typedef enum meta_command_result_t {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED
} MetaCommandResult;

typedef enum prepare_result_t {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED
} PrepareResult;

typedef enum prepared_type_t {
    STATEMENT_INSERT, STATEMENT_SELECT
} StatementType;

typedef struct input_buffer_t {
    char* buffer;
    size_t buffer_len;
    ssize_t input_len;
} InputBuffer;

typedef struct statement_t {
    StatementType type;
} Statement;

void print_prompt();
void read_input(InputBuffer* buffer);
MetaCommandResult do_meta_command(InputBuffer* buffer);
InputBuffer* new_input_buffer();
PrepareResult prepare_statement(InputBuffer* buffer, Statement* statement);
void execute_statement(Statement* statement);

int main() {
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
        case PREPARE_UNRECOGNIZED:
            printf("Unrecognized keyword at start of [%s]\n", input_buffer->buffer);
            continue;
        }

        // We have a real Statement here
        execute_statement(&statement);
    }
}

MetaCommandResult do_meta_command(InputBuffer* buffer) {
    if(strcmp(buffer->buffer, ".exit") == 0) {
        exit(0);
    } else {
        return META_COMMAND_UNRECOGNIZED;
    }
}

PrepareResult prepare_statement(InputBuffer* buffer, Statement* statement) {
    if(strncmp(buffer->buffer, "insert", 6) == 0) { // Starts with 'insert'...followed by more
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }

    if(strcmp(buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED;
}

// This is our VM
void execute_statement(Statement* statement) {
    switch(statement->type) {
    case STATEMENT_INSERT:
        printf("This is where we would do an insert\n");
        break;
    case STATEMENT_SELECT:
        printf("This is where we would do a select\n");
        break;
    }
}

void print_prompt() {
    printf("db> ");
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
