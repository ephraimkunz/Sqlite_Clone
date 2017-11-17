#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

#define IN_BUFFER_SIZE 30

typedef struct input_buffer_t {
    char* buffer;
    size_t buffer_len;
    ssize_t input_len;
} InputBuffer;

void print_prompt();
void read_input(InputBuffer* buffer);
InputBuffer* new_input_buffer();

int main() {
    InputBuffer* input_buffer = new_input_buffer();
    while(true) {
        print_prompt();
        read_input(input_buffer);

        if(strcmp(input_buffer->buffer, ".exit") == 0) {
            exit(0);
        } else {
            printf("Unrecognized command [%s]\n", input_buffer->buffer);
        }
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
