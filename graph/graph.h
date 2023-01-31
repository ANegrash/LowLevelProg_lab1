#ifndef LAB1_GRAPH_H
#define LAB1_GRAPH_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

enum database_types{
    INT32 = 1,
    REAL = 2,
    STRING = 3,
    BOOLEAN = 4
};

struct attribute{
    char* name;
    enum database_types type;
    union
    {
        int32_t int32;
        float real;
        char* string;
        bool boolean;
    } value;

};

struct connection{
    struct node* source;
    struct node* target;
    char* name;
};

struct node{
    int id;
    size_t num_of_connections;
    struct connection* connections;
    size_t num_of_attributes;
    struct attribute* attributes;
};

struct request_attribute{
    struct attribute attribute;
    char* operator;
};

struct request{
    size_t node_id; //соответствует ID ноды, или 0, если ID ноды не был указан
    size_t num_of_source_attributes;
    struct request_attribute* request_source_attributes;
    size_t num_of_connection_names;
    char** connection_name; //соответствует имени связи или "", если имя связи не указано
    size_t* connection_id;
    char* connection_role;
    size_t num_of_request_attributes;
    struct request_attribute request_target_attributes[]; //атрибуты, значения и операторы
};

enum actions{
    ADD = 0,
    REMOVE = 1,
    UPDATE = 2,
    GET = 3
};

struct user_request{
    enum actions act;
    struct node* node;
    struct request* req;
};

struct user_answer{
    bool is_success;
    struct node* node;
    char* message;
};

struct database_node {
    size_t node_id;
    size_t num_attributes;
    struct attribute* attributes;
    size_t num_of_connections;
    char** connection_names;
    size_t* connection_ids;
    char* connection_roles;
};

typedef uint32_t addres_t;

struct allocators_node {
    int size;
    addres_t addres;
    addres_t last_addres;
    struct allocators_node* next_allocator;
};

struct database_struct{
    FILE* file_descriptor;
    addres_t addres_block;
    addres_t node_block;
    addres_t relations_block;
    struct allocators_node* node_allocator;
    struct allocators_node* relations_allocator;
};

struct database_struct* open_database(char* filename);
void close_database(struct database_struct* db);
struct user_answer do_request(struct database_struct* db, struct user_request* ur);

#endif
