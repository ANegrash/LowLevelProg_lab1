#include "graph.h"
#include <string.h>
#include <stdlib.h>

#define GRAPH_ADDRESS   1
#define GRAPH_NODE      2
#define GRAPH_RELATIONS 3

#define GET_CURRENT_ADDRESS(db)       ftell(db->file_descriptor)
#define GO_TO_ADDRESS(db, addres)    fseek(db->file_descriptor, addres, SEEK_SET)
#define GO_FORWARD(db, offset)    fseek(db->file_descriptor, offset, SEEK_CUR)

#define FSCANF_DB(db, addres, format, size, type, last_addres, args...) \
{ \
    char* str_tmp = malloc((size + 1)* sizeof(char)); \
    last_addres = read_from_db_by_adress(db, addres, str_tmp, size, type); \
    sscanf(str_tmp, format, ##args); \
    free(str_tmp); \
}

#define DEFAULT_BLOCK_SIZE TABLE_NODE_SIZE * 50

#define BLOCK_HEADER_SIZE 24
#define BLOCK_HEADER_FORMAT "%hu;%10zu;%10zu;"

#define BLOCK_TABLE_HEADER_SIZE 35
#define BLOCK_TABLE_HEADER_FORMAT "%hu;%10zu;%10zu;%10zu;"
#define TABLE_NODE_SIZE 34
#define TABLE_NODE_FORMAT "%10zu;%10u;%10u;"
#define TABLE_NODE_FORMAT_FULL "n%10zu;%10u;%10u;"

#define NODE_HEADER_FORMAT "%10zu;%s"
#define NODE_HEADER_FORMAT_FULL "n%10zu;%s"
#define NODE_HEADER_FORMAT_FULL_WITHOUT_STRING "n%10zu;"
#define NODE_PARAM_FORMAT_STRING_SIZES ";%10zu;%10zu;"
#define NODE_PARAM_FORMAT_STRING "s;%10zu;%10zu;%s;%s;"
#define NODE_PARAM_FORMAT_INTEGER "i;%10zu;%s;%10ld;"
#define NODE_PARAM_FORMAT_FLOAT "f;%10zu;%s;%10f;"
#define NODE_PARAM_FORMAT_BOOL "b;%10zu;%s;%c;"

#define RELATIONS_HEADER_FORMAT "%10zu;%s"
#define RELATIONS_HEADER_FORMAT_FULL "n%10zu;%s"
#define RELATIONS_FORMAT_SOURCE "%c;%10zu;%10zu;%s;"
#define RELATIONS_FORMAT_SOURCE_FULL "s%c;%10zu;%10zu;%s;"
#define RELATIONS_FORMAT_TARGET "%c;%10zu;"
#define RELATIONS_FORMAT_TARGET_FULL "t%c;%10zu;"

//#define DEBUG

#ifdef DEBUG
void print_debug(char* str)
{ printf(str); }
#else
void print_debug(char* str)
{ }
#endif

static size_t read_from_db_by_adress(struct database_struct* db, addres_t addres, char* data, size_t size, uint8_t block_type);

struct block_header{
    addres_t addres;
    uint8_t type;
    size_t size;
    addres_t next_block_adress;
};

struct table_header{
    struct block_header header;
    size_t num_of_rows;
};

struct table_node{
    size_t id;
    addres_t nodes;
    addres_t connections;
};

static void read_block_header(FILE* pointer, struct block_header* bh){
    bh->addres = ftell(pointer);
    fscanf(pointer, BLOCK_HEADER_FORMAT, &(bh->type), &(bh->size), &(bh->next_block_adress));
}

static void read_block_table_header(FILE* pointer, struct table_header* bh){
    bh->header.addres = ftell(pointer);
    fscanf(pointer, BLOCK_TABLE_HEADER_FORMAT, &(bh->header.type), &(bh->header.size), &(bh->header.next_block_adress), &(bh->num_of_rows));
}

static int go_to_next_block(FILE* pointer, struct block_header* bh){
    return fseek(pointer, bh->addres + bh->size, SEEK_SET);
}

static int go_to_next_block_same_type(FILE* pointer, struct block_header* bh)
{ return fseek(pointer, bh->next_block_adress, SEEK_SET); }

static addres_t get_first_addres_by_num(struct database_struct* ds, uint8_t num)
{
    switch (num)
    {
        case 1:
            return ds->addres_block;
        case 2:
            return ds->node_block;
        case 3:
            return ds->relations_block;
        default:
            break;
    }
}

static struct allocators_node * get_allocator_by_num(struct database_struct* ds, uint8_t num)
{
    switch (num)
    {
        case 2:
            return ds->node_allocator;
        case 3:
            return ds->relations_allocator;
        default:
            break;
    }
}

void set_allocator_by_num(struct database_struct* ds, uint8_t num, struct allocators_node * node)
{
    switch (num)
    {
        case 2:
            ds->node_allocator = node;
            break;
        case 3:
            ds->relations_allocator = node;
            break;
        default:
            break;
    }
}

static void create_empty_block(struct database_struct* db, uint8_t block_type, size_t size, struct block_header* bh)
{
    bh->addres = GET_CURRENT_ADDRESS(db);
    bh->next_block_adress = 0;
    bh->size = size + BLOCK_HEADER_SIZE;
    bh->type = block_type;
    fprintf(db->file_descriptor, BLOCK_HEADER_FORMAT, block_type, size + BLOCK_HEADER_SIZE, 0);
    char str[size + 1];
    str[0] = 0;
    for (size_t i = 0; i < size; i++)
        strncat(str, "0", 1);
    fprintf(db->file_descriptor, "%s", str);
}

static void create_empty_table_block(struct database_struct* db, uint8_t block_type, size_t size, struct table_header* bh)
{
    bh->header.addres = GET_CURRENT_ADDRESS(db);
    bh->header.next_block_adress = 0;
    bh->header.size = size + BLOCK_TABLE_HEADER_SIZE;
    bh->header.type = block_type;
    bh->num_of_rows = 0;
    fprintf(db->file_descriptor, BLOCK_TABLE_HEADER_FORMAT, block_type, size + BLOCK_TABLE_HEADER_SIZE, 0, 0);
    char* str = (char*) malloc(sizeof(char) * size + 1);
    for (size_t i = 0; i < size; i++)
        strncat(str, "0", size);
    fprintf(db->file_descriptor, "%s", str);
    free(str);
}

static void update_allocators(struct database_struct* db)
{
    struct block_header bh;
    struct allocators_node* an = (struct allocators_node*) malloc(sizeof(struct allocators_node));
    struct allocators_node* prev_an = NULL;
    db->node_allocator = an;
    size_t block_size = 0;
    size_t block_start_addres = 0;
    size_t i;
    GO_TO_ADDRESS(db, db->node_block);
    do
    {
        read_block_header(db->file_descriptor, &bh);
        for (i = bh.addres + BLOCK_HEADER_SIZE; i < bh.addres + bh.size; i++)
        {
            GO_TO_ADDRESS(db, i);
            char c = fgetc(db->file_descriptor);
            if (c == '0')
            {
                if (block_start_addres == 0)
                    block_start_addres = i;
                block_size++;
            }
            else
            {
                if (block_start_addres != 0)
                {
                    an->addres = block_start_addres;
                    an->last_addres = i;
                    an->size = block_size;
                    an->next_allocator = (struct allocators_node*) malloc(sizeof(struct allocators_node));
                    prev_an = an;
                    an = an->next_allocator;
                    block_size = 0;
                    block_start_addres = 0;
                    if (c = 'n')
                    {
                        size_t size;
                        char str[12];
                        addres_t addr;
                        FSCANF_DB(db, GET_CURRENT_ADDRESS(db), "%10zu;", 11, 2, addr, &size);
                        GO_FORWARD(db, size);
                    }
                }
            }
        }
        GO_TO_ADDRESS(db, bh.next_block_adress);
    }
    while(bh.next_block_adress != 0);

    if (block_start_addres != 0)
    {
        an->addres = block_start_addres;
        an->last_addres = i;
        an->size = block_size;
        an->next_allocator = (struct allocators_node*) malloc(sizeof(struct allocators_node));
        prev_an = an;
        an = an->next_allocator;
        block_size = 0;
        block_start_addres = 0;
    }
    free(an);
    prev_an->next_allocator = NULL;


    GO_TO_ADDRESS(db, db->relations_block);
    read_block_header(db->file_descriptor, &bh);
    an = (struct allocators_node*) malloc(sizeof(struct allocators_node));
    db->relations_allocator = an;
    block_size = 0;
    block_start_addres = 0;
    do
    {
        for (i = bh.addres + BLOCK_HEADER_SIZE; i < bh.addres + bh.size; i++)
        {
            GO_TO_ADDRESS(db, i);
            char c = fgetc(db->file_descriptor);
            if (c == '0')
            {
                if (block_start_addres == 0)
                    block_start_addres = i;
                block_size++;
            }
            else
            {
                if (block_start_addres != 0)
                {
                    an->addres = block_start_addres;
                    an->last_addres = i;
                    an->size = block_size;
                    an->next_allocator = (struct allocators_node*) malloc(sizeof(struct allocators_node));
                    prev_an = an;
                    an = an->next_allocator;
                    block_size = 0;
                    block_start_addres = 0;
                    if (c = 'n')
                    {
                        size_t size;
                        char str[12];
                        addres_t addr;
                        FSCANF_DB(db, GET_CURRENT_ADDRESS(db), "%10zu;", 11, 2, addr, &size);
                        GO_FORWARD(db, size);
                    }
                }
            }
        }
        GO_TO_ADDRESS(db, bh.next_block_adress);
        read_block_header(db->file_descriptor, &bh);
    }
    while(bh.next_block_adress != 0);

    if (block_start_addres != 0)
    {
        an->addres = block_start_addres;
        an->last_addres = i;
        an->size = block_size;
        an->next_allocator = (struct allocators_node*) malloc(sizeof(struct allocators_node));
        prev_an = an;
        an = an->next_allocator;
        block_size = 0;
        block_start_addres = 0;
    }

    free(an);
    prev_an->next_allocator = NULL;
}

struct database_struct* open_database(char* filename)
{
    bool set_addres_block = false;
    bool set_node_block = false;
    bool set_relation_block = false;
    bool seek_result = 0;
    struct database_struct* ds = (struct database_struct*)malloc(sizeof(struct database_struct));
    ds->file_descriptor = fopen(filename, "r+");
    if (!ds->file_descriptor)
    {
        print_debug("Файл не существует, создаю новый.\n");
        ds->file_descriptor = fopen(filename, "w+");

        struct table_header th;
        create_empty_table_block(ds, GRAPH_ADDRESS, DEFAULT_BLOCK_SIZE, &th);
        if (go_to_next_block(ds->file_descriptor, &th.header) != 0)
            return NULL;
        ds->addres_block = th.header.addres;

        struct block_header bh;
        create_empty_block(ds, GRAPH_NODE, DEFAULT_BLOCK_SIZE, &bh);
        if (go_to_next_block(ds->file_descriptor, &bh) != 0)
            return NULL;
        ds->node_block = bh.addres;
        ds->node_allocator = (struct allocators_node*) malloc(sizeof(struct allocators_node));
        ds->node_allocator->addres = bh.addres + BLOCK_HEADER_SIZE;
        ds->node_allocator->size = DEFAULT_BLOCK_SIZE;
        ds->node_allocator->last_addres = bh.addres + BLOCK_HEADER_SIZE + DEFAULT_BLOCK_SIZE;
        ds->node_allocator->next_allocator = NULL;

        create_empty_block(ds, GRAPH_RELATIONS, DEFAULT_BLOCK_SIZE, &bh);
        if (go_to_next_block(ds->file_descriptor, &bh) != 0)
            return NULL;
        ds->relations_block = bh.addres;
        ds->relations_allocator = (struct allocators_node*) malloc(sizeof(struct allocators_node));
        ds->relations_allocator->addres = bh.addres + BLOCK_HEADER_SIZE;
        ds->relations_allocator->size = DEFAULT_BLOCK_SIZE;
        ds->relations_allocator->last_addres = bh.addres + BLOCK_HEADER_SIZE + DEFAULT_BLOCK_SIZE;
        ds->relations_allocator->next_allocator = NULL;
        print_debug("Успешно создан\n");
        return ds;
    }
    else
        print_debug("Открываю сущетсвующую базу данных\n");

    while ((!set_addres_block || !set_node_block || !set_relation_block) && seek_result == 0){
        struct block_header block_header;
        read_block_header(ds->file_descriptor, &block_header);
        switch (block_header.type)
        {
            case GRAPH_ADDRESS:
                print_debug("Найден первый блок ADDRES\n");
                if (set_addres_block)
                    break;
                ds->addres_block = block_header.addres;
                set_addres_block = true;
                break;
            case GRAPH_NODE:
                print_debug("Найден первый блок NODE\n");
                if (set_node_block)
                    break;
                ds->node_block = block_header.addres;
                set_node_block = true;
                break;
            case GRAPH_RELATIONS:
                print_debug("Найден первый блок RELATIONS\n");
                if (set_relation_block)
                    break;
                ds->relations_block = block_header.addres;
                set_relation_block = true;
                break;
            default:
                break;
        }
        seek_result = go_to_next_block(ds->file_descriptor, &block_header);
    }
    update_allocators(ds);
    return ds;
}

void close_database(struct database_struct* db)
{
    fclose(db->file_descriptor);
    free(db);
}

static addres_t allocate_new_block(struct database_struct* db, uint8_t block_type, size_t size){
    fseek(db->file_descriptor, 0, SEEK_END);

    struct block_header bh;
    struct table_header th;
    if (block_type == 1)
    {
        create_empty_table_block(db, block_type, size, &th);
        bh = th.header;
    }
    else
        create_empty_block(db, block_type, size, &bh);
    addres_t new_block_addres = bh.addres;
    addres_t new_block_last_addres = bh.addres + bh.size;
    print_debug("Блок алоцирован, меняю предыдущий блок.\n");

    addres_t addres = get_first_addres_by_num(db, block_type);
    GO_TO_ADDRESS(db, addres);
    read_block_header(db->file_descriptor, &bh);
    print_debug("Начинаю поиск последнего блока\n");
    while(bh.next_block_adress != 0)
    {
        GO_TO_ADDRESS(db, bh.next_block_adress);
        read_block_header(db->file_descriptor, &bh);
    }
    GO_TO_ADDRESS(db, bh.addres);
    fprintf(db->file_descriptor, BLOCK_HEADER_FORMAT, bh.type, bh.size, new_block_addres);
    if (block_type != 1)
    {
        addres_t last_addres = bh.addres + bh.size;
        struct allocators_node* node = get_allocator_by_num(db, block_type);
        struct allocators_node* prev_node = get_allocator_by_num(db, block_type);
        prev_node = node;
        while (node != NULL)
        {
            if (node->last_addres == last_addres)
            {
                print_debug("Нашел последний блок в аллокаторе\n");
                node->size += size;
                node->last_addres = new_block_last_addres;
                break;
            }
            prev_node = node;
            node = node->next_allocator;
        }
        if (node == NULL)
        {
            print_debug("Не нашел последний блок в аллокаторе\n");
            if (prev_node == NULL)
            {
                struct allocators_node* new_node = (struct allocators_node*)malloc(sizeof(struct allocators_node));
                new_node->addres = new_block_addres + BLOCK_HEADER_SIZE;
                new_node->last_addres = new_block_last_addres;
                new_node->size = size;
                new_node->next_allocator = NULL;
                set_allocator_by_num(db, block_type, new_node);

            }
            else
            {
                prev_node->next_allocator = (struct allocators_node*)malloc(sizeof(struct allocators_node));
                prev_node->next_allocator->addres = new_block_addres + BLOCK_HEADER_SIZE;
                prev_node->next_allocator->last_addres = new_block_last_addres;
                prev_node->next_allocator->size = size;
                prev_node->next_allocator->next_allocator = NULL;
            }
        }
    }

    return new_block_addres;
}

static addres_t write_to_db_by_adress(struct database_struct* db, addres_t addres, char* data, size_t size, uint8_t block_type)
{
    GO_TO_ADDRESS(db, get_first_addres_by_num(db, block_type));

    struct block_header bh;
    read_block_header(db->file_descriptor, &bh);
    while (bh.next_block_adress != 0 && bh.addres + bh.size < addres)
    {
        GO_TO_ADDRESS(db, bh.next_block_adress);
        read_block_header(db->file_descriptor, &bh);
    }
    print_debug("Найден блок куда писать\n");
    if (bh.addres + bh.size < addres + size)
    {
        print_debug("Запись не влезет в этот блок(\n");
        if (bh.next_block_adress == 0)
            allocate_new_block(db, block_type, DEFAULT_BLOCK_SIZE);
        GO_TO_ADDRESS(db, bh.addres);
        read_block_header(db->file_descriptor, &bh);
        int32_t left_in_block_size = size - ((addres + size) - (bh.addres + bh.size));

        addres_t last_addres = write_to_db_by_adress(db, bh.next_block_adress + BLOCK_HEADER_SIZE, data + left_in_block_size, size - left_in_block_size, block_type);
        GO_TO_ADDRESS(db, addres);
        fwrite(data, left_in_block_size, 1, db->file_descriptor);
        return last_addres;
    }
    else
    {
        GO_TO_ADDRESS(db, addres);
        fwrite(data, size, 1, db->file_descriptor);
        return addres + size;
    }
}

static size_t read_from_db_by_adress(struct database_struct* db, addres_t addres, char* data, size_t size, uint8_t block_type)
{
    GO_TO_ADDRESS(db, get_first_addres_by_num(db, block_type));

    struct block_header bh;
    read_block_header(db->file_descriptor, &bh);
    while (bh.next_block_adress != 0 && bh.addres + bh.size < addres)
    {
        GO_TO_ADDRESS(db, bh.next_block_adress);
        read_block_header(db->file_descriptor, &bh);
    }
    print_debug("Найден блок куда читать\n");
    if (bh.addres + bh.size < addres + size)
    {
        print_debug("Запись лежит в несколких блоках(\n");
        if (bh.next_block_adress == 0)
            allocate_new_block(db, block_type, DEFAULT_BLOCK_SIZE);
        GO_TO_ADDRESS(db, bh.addres);
        read_block_header(db->file_descriptor, &bh);
        int32_t left_in_block_size = size - ((addres + size) - (bh.addres + bh.size));

        addres_t last_addr = read_from_db_by_adress(db, bh.next_block_adress + BLOCK_HEADER_SIZE, data + left_in_block_size, size - left_in_block_size, block_type);
        GO_TO_ADDRESS(db, addres);
        fread(data, left_in_block_size, 1, db->file_descriptor);
        return last_addr;
    }
    else
    {
        GO_TO_ADDRESS(db, addres);
        fread(data, size, 1, db->file_descriptor);
        return addres + size;
    }
}

static addres_t callculate_last_addres(struct database_struct* db, addres_t addres, size_t size, uint8_t block_type)
{
    GO_TO_ADDRESS(db, get_first_addres_by_num(db, block_type));

    struct block_header bh;
    read_block_header(db->file_descriptor, &bh);
    while (bh.next_block_adress != 0 && bh.addres + bh.size < addres)
    {
        GO_TO_ADDRESS(db, bh.next_block_adress);
        read_block_header(db->file_descriptor, &bh);
    }

    if (bh.addres + bh.size < addres + size)
    {
        int32_t left_in_block_size = size - ((addres + size) - (bh.addres + bh.size));

        addres_t last_addres = callculate_last_addres(db, bh.next_block_adress + BLOCK_HEADER_SIZE, size - left_in_block_size, block_type);

        return last_addres;
    }
    else
        return addres + size;
}

static addres_t add_string_to_block(struct database_struct* db, char* str, size_t size, uint8_t block)
{
    struct allocators_node* node;
    struct allocators_node* prev_node = NULL;
    struct allocators_node* min_node = NULL;
    struct allocators_node* prev_min_node = NULL;

    while (min_node == NULL)
    {
        node = get_allocator_by_num(db, block);

        if (node == NULL)
        {
            allocate_new_block(db, block, DEFAULT_BLOCK_SIZE);
            node = get_allocator_by_num(db, block);
        }

        while (node != NULL)
        {
            if (node->size >= size)
            {
                if (min_node == NULL || min_node->size > node->size)
                {
                    prev_min_node = prev_node;
                    min_node = node;
                }
            }
            prev_node = node;
            node = node->next_allocator;
        }
        if (min_node == NULL)
            allocate_new_block(db, block, DEFAULT_BLOCK_SIZE);
    }
    addres_t start_addres = min_node->addres;
    addres_t last_addres = write_to_db_by_adress(db, min_node->addres, str, size, block);
    if (size == min_node->size)
    {
        if (prev_min_node == NULL && min_node->next_allocator == NULL)
        {
            set_allocator_by_num(db, block, NULL);
            free(min_node);
            return start_addres;
        }

        if (prev_min_node == NULL)
        {
            set_allocator_by_num(db, block, min_node->next_allocator);
            free(min_node);
            return start_addres;
        }

        if (min_node->next_allocator == NULL)
        {
            prev_min_node->next_allocator = NULL;
            free(min_node);
            return start_addres;
        }

        prev_min_node->next_allocator = min_node->next_allocator;
        free(min_node);
    }
    else
    {
        min_node->size -= size;
        min_node->addres = last_addres;
    }

    return start_addres;
}

struct table_node_with_address{
    addres_t addres;
    bool    is_exist;
    struct table_node node;
};

static void read_table_node(struct database_struct* db, struct table_node_with_address* tn){
    tn->addres = GET_CURRENT_ADDRESS(db);

    char ch = fgetc(db->file_descriptor);
    if (ch == 'n')
    {
        fscanf(db->file_descriptor, TABLE_NODE_FORMAT, &(tn->node.id), &(tn->node.nodes), &(tn->node.connections));
        tn->is_exist = true;
    }
    else
        tn->is_exist = false;
}

void write_table_node(struct database_struct* db, struct table_node* node){
    fprintf(db->file_descriptor, TABLE_NODE_FORMAT_FULL, node->id, node->nodes, node->connections);
}

void add_node_to_table(struct database_struct* db, struct table_node* node)
{
    GO_TO_ADDRESS(db, get_first_addres_by_num(db, 1));
    struct table_header th;
    read_block_table_header(db->file_descriptor, &th);

    while(th.num_of_rows == (th.header.size - BLOCK_TABLE_HEADER_SIZE) / TABLE_NODE_SIZE && th.header.next_block_adress != 0)
    {
        GO_TO_ADDRESS(db, th.header.next_block_adress);
        read_block_table_header(db->file_descriptor, &th);
    }

    if (th.num_of_rows == (th.header.size - BLOCK_TABLE_HEADER_SIZE) / TABLE_NODE_SIZE)
    {
        print_debug("Нет места в блоке, добавляю блок");
        addres_t block_addres = allocate_new_block(db, 1, DEFAULT_BLOCK_SIZE);
        GO_TO_ADDRESS(db, block_addres);
        read_block_table_header(db->file_descriptor, &th);
        write_table_node(db, node);
    }
    else
    {
        struct table_node_with_address tn;
        read_table_node(db, &tn);
        while (tn.is_exist == true)
            read_table_node(db, &tn);

        GO_TO_ADDRESS(db, tn.addres);
        write_table_node(db, node);
    }
    GO_TO_ADDRESS(db, th.header.addres);
    th.num_of_rows++;
    fprintf(db->file_descriptor, BLOCK_TABLE_HEADER_FORMAT, th.header.type, th.header.size, th.header.next_block_adress, th.num_of_rows);
}

addres_t find_node(struct database_struct* db, size_t id)
{
    struct table_header th;
    th.header.next_block_adress = get_first_addres_by_num(db, 1);

    do
    {
        GO_TO_ADDRESS(db, th.header.next_block_adress);

        read_block_table_header(db->file_descriptor, &th);
        size_t num_of_nodes = 0;
        do
        {
            struct table_node_with_address tn;
            read_table_node(db, &tn);

            if (tn.is_exist)
            {
                num_of_nodes++;
                if (tn.node.id == id)
                    return tn.addres;
            }
            if (num_of_nodes == th.num_of_rows)
                break;
        }
        while(GET_CURRENT_ADDRESS(db) < th.header.addres + th.header.size);
    }
    while(th.header.next_block_adress != 0);

    return 0;
}

static char* attribute_to_str(struct attribute atr)
{
    char* output_str;
    char* atr_name = atr.name;
    switch (atr.type)
    {
        case INT32:
        {
            int32_t value = atr.value.int32;
            output_str = (char*) malloc(sizeof(char) * (strlen(atr_name) + 40));
            sprintf(output_str, NODE_PARAM_FORMAT_INTEGER, strlen(atr_name), atr_name, value);
            break;
        }
        case REAL:
        {
            float value = atr.value.real;
            output_str = (char*) malloc(sizeof(char) * (strlen(atr_name) + 1 + 10 + 1 + 2 + 1));
            sprintf(output_str, NODE_PARAM_FORMAT_FLOAT, strlen(atr_name), atr_name, value);
            break;
        }
        case STRING:
        {
            char* value = atr.value.string;
            output_str = (char*) malloc(sizeof(char) * (strlen(atr_name) + strlen(value) + 4 + 1));
            sprintf(output_str, NODE_PARAM_FORMAT_STRING, strlen(atr_name), strlen(value), atr_name, value);
            break;
        }
        case BOOLEAN:
        {
            bool value = atr.value.boolean;
            output_str = (char*) malloc(sizeof(char) * (strlen(atr_name) + 1 + 1 + 1 + 2 + 1));
            sprintf(output_str, NODE_PARAM_FORMAT_BOOL, strlen(atr_name), atr_name, value ? 't' : 'f');
            break;
        }
        default:
            break;
    }
    return output_str;
}

static char* add_node;

static bool add_to_db(struct database_struct* db, struct request* req){
    if(find_node(db, req->node_id) != 0)
        return 0;

    add_node = malloc(sizeof(char) * 1);

    add_node[0] = 0;
    for (size_t i = 0; i < req->num_of_source_attributes; i++)
    {
        struct request_attribute ra = req->request_source_attributes[i];
        char* node_str = attribute_to_str(ra.attribute);
        size_t node_size = strlen(add_node);
        char copy_str[node_size + 1];
        strcpy(copy_str, add_node);
        free(add_node);
        add_node = (char*) calloc((node_size + strlen(node_str)), sizeof(char));
        sprintf(add_node, "%s%s", copy_str, node_str);
        free(node_str);
    }

    size_t size = strlen(add_node) + 12;
    char* result_str = (char*) malloc(sizeof(char) * (size + 1));
    sprintf(result_str, NODE_HEADER_FORMAT_FULL, size, add_node);
    free(add_node);

    addres_t node_addres = add_string_to_block(db, result_str, size, GRAPH_NODE);
    char* connections = (char*)malloc(1);
    for (size_t i = 0; i < req->num_of_connection_names; i++)
    {
        char* tmpstr;
        if (req->connection_role[i] == 's')
        {
            tmpstr = (char*) malloc(sizeof(char) * 16 + strlen(req->connection_name[i]));
            sprintf(tmpstr, RELATIONS_FORMAT_SOURCE_FULL, 'n', req->connection_id[i], strlen(req->connection_name[i]), req->connection_name[i]);
        }
        else
        {
            tmpstr = (char*) malloc(sizeof(char) * 16);
            sprintf(tmpstr, RELATIONS_FORMAT_TARGET_FULL, 'n', req->connection_id[i]);
        }

        connections = (char*)realloc(connections, (strlen(connections) + strlen(tmpstr) + 1));
        strncat(connections, tmpstr, strlen(tmpstr));
        free(tmpstr);
    }
    size = strlen(connections) + 12;
    result_str = (char*) malloc(sizeof(char) * size);
    sprintf(result_str, RELATIONS_HEADER_FORMAT_FULL, size, connections);
    free(connections);

    addres_t relations_addres = add_string_to_block(db, result_str, size, GRAPH_RELATIONS);

    struct table_node tn = {req->node_id, node_addres, relations_addres};
    add_node_to_table(db, &tn);

    return 1;
}

static struct database_node * read_node(struct database_struct* db, struct table_node_with_address* tn, struct database_node* dn)
{
    dn->node_id = tn->node.id;

    GO_TO_ADDRESS(db, tn->node.nodes);
    size_t size;

    addres_t last_addres = tn->node.nodes;
    FSCANF_DB(db, last_addres, NODE_HEADER_FORMAT_FULL_WITHOUT_STRING, 12, GRAPH_NODE, last_addres, &size);

    dn->num_attributes = 0;
    dn->num_of_connections = 0;
    dn->attributes = malloc(0);

    addres_t last_addr = callculate_last_addres(db, tn->node.nodes, size, GRAPH_NODE);
    while(last_addres < last_addr)
    {
        char ch;
        FSCANF_DB(db, last_addres, "%c", 1, GRAPH_NODE, last_addres, &ch);
        switch (ch)
        {
            case 's':
            {
                size_t name_size;
                size_t value_size;

                FSCANF_DB(db, last_addres, NODE_PARAM_FORMAT_STRING_SIZES, 23, GRAPH_NODE, last_addres, &name_size, &value_size);
                char* name = (char*)malloc((name_size + 1) * sizeof(char));
                char* value = (char*)malloc((value_size + 1) * sizeof(char));
                char tmp_ch;
                FSCANF_DB(db, last_addres, "%s", name_size, GRAPH_NODE, last_addres, name);
                name[name_size] = 0;
                FSCANF_DB(db, last_addres, "%c", name_size, GRAPH_NODE, last_addres, &tmp_ch);
                FSCANF_DB(db, last_addres, "%s", value_size, GRAPH_NODE, last_addres, value);
                value[value_size] = 0;
                FSCANF_DB(db, last_addres, "%c", name_size, GRAPH_NODE, last_addres, &tmp_ch);

                dn->num_attributes++;
                dn->attributes = realloc(dn->attributes, sizeof(struct attribute) * dn->num_attributes);
                dn->attributes[dn->num_attributes - 1].name = name;
                dn->attributes[dn->num_attributes - 1].value.string = value;
                dn->attributes[dn->num_attributes - 1].type = STRING;
                break;
            }
            case 'i':
            {
                size_t name_size;

                FSCANF_DB(db, last_addres, ";%10zu;", 12, GRAPH_NODE, last_addres, &name_size);
                char* name = (char*)malloc((name_size + 1) * sizeof(char));
                int32_t value;
                FSCANF_DB(db, last_addres, "%s", name_size, GRAPH_NODE, last_addres, name);
                name[name_size] = 0;
                FSCANF_DB(db, last_addres, ";%10lld;", 2 + 10, GRAPH_NODE, last_addres, &value);

                dn->num_attributes++;
                dn->attributes = realloc(dn->attributes, sizeof(struct attribute) * dn->num_attributes);
                dn->attributes[dn->num_attributes - 1].name = name;
                dn->attributes[dn->num_attributes - 1].value.int32 = value;
                dn->attributes[dn->num_attributes - 1].type = INT32;
                break;
            }
            case 'f':
            {
                size_t name_size;

                FSCANF_DB(db, last_addres, ";%10zu;", 2 + 10, GRAPH_NODE, last_addres, &name_size);
                char* name = (char*)malloc((name_size + 1) * sizeof(char));
                name[name_size] = 0;
                float value;
                FSCANF_DB(db, last_addres, "%s", name_size, GRAPH_NODE, last_addres, name);
                name[name_size] = 0;
                FSCANF_DB(db, last_addres, ";%10f;", 2 + 10, GRAPH_NODE, last_addres, &value);

                dn->num_attributes++;
                dn->attributes = realloc(dn->attributes, sizeof(struct attribute) * dn->num_attributes);
                dn->attributes[dn->num_attributes - 1].name = name;
                dn->attributes[dn->num_attributes - 1].value.real = value;
                dn->attributes[dn->num_attributes - 1].type = REAL;
                break;
            }
            case 'b':
            {
                size_t name_size;

                FSCANF_DB(db, last_addres, ";%10zu;", 12, GRAPH_NODE, last_addres, &name_size);
                char* name = (char*)malloc((name_size + 1) * sizeof(char));
                name[name_size] = 0;
                char value;
                FSCANF_DB(db, last_addres, "%s", name_size, GRAPH_NODE, last_addres, name);
                name[name_size] = 0;
                FSCANF_DB(db, last_addres, ";%c;", 3, GRAPH_NODE, last_addres, &value);

                dn->num_attributes++;
                dn->attributes = realloc(dn->attributes, sizeof(struct attribute) * dn->num_attributes);
                dn->attributes[dn->num_attributes - 1].name = name;
                dn->attributes[dn->num_attributes - 1].value.boolean = value == 't' ? true : false;
                dn->attributes[dn->num_attributes - 1].type = BOOLEAN;
                break;
            }
            default:
                break;
        }
    }

    FSCANF_DB(db, tn->node.connections, "%10zu;", 11, GRAPH_RELATIONS, last_addres, &size);

    last_addr = callculate_last_addres(db, tn->node.connections, size, GRAPH_RELATIONS);
    last_addres = tn->node.connections;
    dn->connection_ids = malloc(0);
    dn->connection_names = malloc(0);
    dn->connection_roles = malloc(0);
    while(last_addres < last_addr)
    {
        char ch;
        FSCANF_DB(db, last_addres, "%c", 1, GRAPH_RELATIONS, last_addres, &ch);
        switch (ch)
        {
            case 's':
            {
                size_t address;
                size_t name_size;

                FSCANF_DB(db, last_addres, "%10zu;%10zu;", 22, GRAPH_RELATIONS, last_addres, &address, &name_size);
                char* name = malloc((name_size + 1) * sizeof(char));
                char tmp_ch;
                FSCANF_DB(db, last_addres, "%s", name_size, GRAPH_RELATIONS, last_addres, name);
                name[name_size] = 0;
                FSCANF_DB(db, last_addres, "%c", name_size, GRAPH_RELATIONS, last_addres, &tmp_ch);

                dn->num_of_connections++;
                dn->connection_ids = realloc(dn->connection_ids, sizeof(size_t) * dn->num_of_connections);
                dn->connection_names = realloc(dn->connection_names, sizeof(char*) * dn->num_of_connections);
                dn->connection_roles = realloc(dn->connection_roles, sizeof(char) * dn->num_of_connections);
                dn->connection_ids[dn->num_of_connections - 1] = address;
                dn->connection_names[dn->num_of_connections - 1] = name;
                dn->connection_roles[dn->num_of_connections - 1] = 's';
                break;
            }
            case 't':
            {
                size_t address;
                FSCANF_DB(db, last_addres, "%10zu;", 11, GRAPH_RELATIONS, last_addres, &address);

                dn->num_of_connections++;
                dn->connection_ids = realloc(dn->connection_ids, sizeof(size_t) * dn->num_of_connections);
                dn->connection_names = realloc(dn->connection_names, sizeof(char*) * dn->num_of_connections);
                dn->connection_roles = realloc(dn->connection_roles, sizeof(char) * dn->num_of_connections);
                dn->connection_ids[dn->num_of_connections - 1] = address;
                dn->connection_names[dn->num_of_connections - 1] = "";
                dn->connection_roles[dn->num_of_connections - 1] = 't';
                break;
            }
            default:
                break;
        }
    }
    return dn;
}

static bool compare_attributes(struct request_attribute* req_attr, struct attribute* attr)
{
    if (0 == strcmp(req_attr->attribute.name, attr->name)
        && req_attr->attribute.type == attr->type)
    {
        switch (attr->type)
        {
            case INT32:
            {
                if (strcmp(req_attr->operator, "=") == 0)
                {
                    if (attr->value.int32 == req_attr->attribute.value.int32)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, ">") == 0)
                {
                    if (attr->value.int32 > req_attr->attribute.value.int32)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, ">=") == 0)
                {
                    if (attr->value.int32 >= req_attr->attribute.value.int32)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "<") == 0)
                {
                    if (attr->value.int32 < req_attr->attribute.value.int32)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "<=") == 0)
                {
                    if (attr->value.int32 <= req_attr->attribute.value.int32)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "!=") == 0)
                {
                    if (attr->value.int32 != req_attr->attribute.value.int32)
                        return true;
                    return false;
                }
            }
                break;
            case STRING:
            {
                int cmp_result = strcmp(attr->value.string, req_attr->attribute.value.string);
                if (strcmp(req_attr->operator, "=") == 0)
                {
                    if (cmp_result == 0)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, ">") == 0)
                {
                    if (cmp_result > 0)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, ">=") == 0)
                {
                    if (cmp_result >= 0)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "<") == 0)
                {
                    if (cmp_result < 0)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "<=") == 0)
                {
                    if (cmp_result <= 0)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "!=") == 0)
                {
                    if (cmp_result != 0)
                        return true;
                    return false;
                }
            }
                break;
            case REAL:
            {
                if (strcmp(req_attr->operator, "=") == 0)
                {
                    if (attr->value.real == req_attr->attribute.value.real)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, ">") == 0)
                {
                    if (attr->value.real > req_attr->attribute.value.real)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, ">=") == 0)
                {
                    if (attr->value.real >= req_attr->attribute.value.real)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "<") == 0)
                {
                    if (attr->value.real < req_attr->attribute.value.real)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "<=") == 0)
                {
                    if (attr->value.real <= req_attr->attribute.value.real)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "!=") == 0)
                {
                    if (attr->value.real != req_attr->attribute.value.real)
                        return true;
                    return false;
                }
            }
                break;
            case BOOLEAN:
            {
                if (strcmp(req_attr->operator, "=") == 0)
                {
                    if (attr->value.boolean == req_attr->attribute.value.boolean)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, ">") == 0)
                {
                    if (attr->value.boolean > req_attr->attribute.value.boolean)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, ">=") == 0)
                {
                    if (attr->value.boolean >= req_attr->attribute.value.boolean)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "<") == 0)
                {
                    if (attr->value.boolean < req_attr->attribute.value.boolean)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "<=") == 0)
                {
                    if (attr->value.boolean <= req_attr->attribute.value.boolean)
                        return true;
                    return false;
                }
                if (strcmp(req_attr->operator, "!=") == 0)
                {
                    if (attr->value.boolean != req_attr->attribute.value.boolean)
                        return true;
                    return false;
                }
            }
                break;
            default:
                break;
        }
    }
    return false;
}

struct get_result{
    size_t size;
    struct database_node* nodes;
};

static struct get_result get_from_db(struct database_struct* db, struct request* req){
    struct get_result result;
    struct database_node* dn = malloc(sizeof(struct database_node));

    if (req->node_id != 0)
    {
        addres_t node_addres = find_node(db, req->node_id);

        if (node_addres == 0)
        {
            result.nodes = NULL;
            result.size = 0;
            return result;
        }

        GO_TO_ADDRESS(db, node_addres);
        struct table_node_with_address tn;
        read_table_node(db, &tn);
        GO_TO_ADDRESS(db, tn.node.nodes);

        read_node(db, &tn, dn);
        result.size = 1;
        result.nodes = dn;
    }
    else
    {
        result.size = 0;
        result.nodes = malloc(0);
        GO_TO_ADDRESS(db, get_first_addres_by_num(db, 1));

        struct table_header th;
        addres_t addr = GET_CURRENT_ADDRESS(db);
        do
        {
            GO_TO_ADDRESS(db, addr);
            read_block_table_header(db->file_descriptor, &th);
            addr = GET_CURRENT_ADDRESS(db);
            do
            {
                struct table_node_with_address tn;
                read_table_node(db, &tn);
                if (tn.is_exist)
                {
                    struct database_node dn_tmp;
                    read_node(db, &tn, &dn_tmp);

                    bool is_same = true;

                    for (size_t i = 0; i < dn_tmp.num_attributes; i++)
                    {
                        bool is_same_tmp = false;
                        for (size_t j = 0; j < req->num_of_source_attributes; j++)
                        {
                            if (compare_attributes(req->request_source_attributes + j, dn_tmp.attributes + i))
                                is_same_tmp = true;
                        }
                        if (!is_same_tmp)
                        {
                            is_same = false;
                            break;
                        }
                    }
                    if (is_same)
                    {
                        result.size++;
                        result.nodes = realloc(result.nodes, result.size * sizeof(struct database_node));
                        result.nodes[result.size-1] = dn_tmp;
                    }
                }
            }
            while(GET_CURRENT_ADDRESS(db) < th.header.addres + th.header.size);
        }
        while(th.header.next_block_adress != 0);
    }
    return result;
}

static void remove_node(struct database_struct* db, struct table_node_with_address* tn)
{
    GO_TO_ADDRESS(db, tn->node.nodes);
    size_t node_size;

    addres_t last_address;
    FSCANF_DB(db, tn->node.nodes, "n%10zu;", 12, GRAPH_NODE, last_address, &node_size);

    char* str = malloc(sizeof(char) * (node_size + 1));
    str[0] ='0';
    str[1] = 0;
    for (size_t i = 0; i < node_size; i++)
        strncat(str, "0", node_size);

    last_address = write_to_db_by_adress(db, tn->node.nodes, str, node_size, GRAPH_NODE);

    struct allocators_node* all_node = NULL;
    bool found = false;
    do
    {
        if (all_node == NULL)
            all_node = db->node_allocator;
        else
            all_node = all_node->next_allocator;
        if (all_node->last_addres == tn->node.nodes)
        {
            all_node->size += node_size;
            all_node->last_addres = last_address;
            found = true;
            break;
        }

        if (all_node->addres == last_address)
        {
            all_node += node_size;
            all_node->addres = tn->node.nodes;
            found = true;
            break;
        }
    }
    while (all_node->next_allocator != NULL);

    if (!found)
    {
        struct allocators_node* new_all= malloc(sizeof(struct allocators_node));
        new_all->addres = tn->node.nodes;
        new_all->last_addres = last_address;
        new_all->size = node_size;
        new_all->next_allocator = NULL;
        all_node->next_allocator = new_all;
    }

    GO_TO_ADDRESS(db, tn->node.connections);
    size_t connection_size;

    FSCANF_DB(db, tn->node.connections, "n%10zu;", 12, GRAPH_RELATIONS, last_address, &connection_size);

    char* connections_str = malloc(sizeof(char) * (connection_size + 1));
    connections_str[0] = '0';
    connections_str[1] = 0;
    for (size_t i = 0; i < connection_size; i++)
        strncat(connections_str, "0", connection_size);

    last_address = write_to_db_by_adress(db, tn->node.connections, connections_str, connection_size, GRAPH_RELATIONS);

    all_node = NULL;
    found = false;
    do
    {
        if (all_node == NULL)
            all_node = db->relations_allocator;
        else
            all_node = all_node->next_allocator;
        if (all_node->last_addres == tn->node.nodes)
        {
            all_node->size += node_size;
            all_node->last_addres = last_address;
            found = true;
            break;
        }

        if (all_node->addres == last_address)
        {
            all_node += node_size;
            all_node->addres = tn->node.nodes;
            found = true;
            break;
        }
    }
    while (all_node->next_allocator != NULL);

    if (!found)
    {
        struct allocators_node* new_all= malloc(sizeof(struct allocators_node));
        new_all->addres = tn->node.nodes;
        new_all->last_addres = last_address;
        new_all->size = node_size;
        new_all->next_allocator = NULL;
        all_node->next_allocator = new_all;
    }

    GO_TO_ADDRESS(db, tn->addres);
    fprintf(db->file_descriptor,"0000000000000000000000000000000000");

    GO_TO_ADDRESS(db, db->addres_block);

    struct table_header th;
    read_block_table_header(db->file_descriptor, &th);
    while(th.header.next_block_adress != 0 && th.header.next_block_adress > tn->addres)
    {
        GO_TO_ADDRESS(db, th.header.next_block_adress);
        read_block_table_header(db->file_descriptor, &th);
    }

    th.num_of_rows--;
    GO_TO_ADDRESS(db, th.header.addres);
    fprintf(db->file_descriptor, BLOCK_TABLE_HEADER_FORMAT, th.header.type, th.header.size, th.header.next_block_adress, th.num_of_rows);
}

static bool delete_from_db(struct database_struct* db, struct request* req){
    if (req->node_id != 0)
    {
        addres_t node_addres = find_node(db, req->node_id);

        if (node_addres == 0)
            return false;

        GO_TO_ADDRESS(db, node_addres);
        struct table_node_with_address tn;
        read_table_node(db, &tn);

        remove_node(db, &tn);
        return true;
    }
    else
    {
        GO_TO_ADDRESS(db, get_first_addres_by_num(db, 1));

        struct table_header th;
        addres_t addr = GET_CURRENT_ADDRESS(db);
        bool was_any_deleted = false;
        do
        {
            GO_TO_ADDRESS(db, addr);

            read_block_table_header(db->file_descriptor, &th);
            addr = GET_CURRENT_ADDRESS(db);
            do
            {
                struct table_node_with_address tn;
                read_table_node(db, &tn);
                if (tn.is_exist)
                {
                    struct database_node dn_tmp;
                    read_node(db, &tn, &dn_tmp);

                    bool is_same = true;

                    for (size_t i = 0; i < dn_tmp.num_attributes; i++)
                    {
                        bool is_same_tmp = false;
                        for (size_t j = 0; j < req->num_of_source_attributes; j++)
                        {
                            if (compare_attributes(req->request_source_attributes + j, dn_tmp.attributes + i))
                                is_same_tmp = true;
                        }
                        if (!is_same_tmp)
                        {
                            is_same = false;
                            break;
                        }
                    }
                    if (is_same)
                    {
                        was_any_deleted = true;
                        remove_node(db, &tn);
                        break;
                    }
                }
            }
            while(GET_CURRENT_ADDRESS(db) < th.header.addres + th.header.size);
        }
        while(th.header.next_block_adress != 0);

        if (was_any_deleted)
            return true;
        return false;
    }
}

struct index_list{
    size_t index;
    struct index_list* next_list;
};

struct user_answer do_request(struct database_struct* db, struct user_request* ur)
{
    switch (ur->act)
    {
        case ADD:
        {
            print_debug("Запрос на добавление\n");
            if (ur->req != NULL)
            {
                print_debug("Делаю прямой запрос\n");
                if (add_to_db(db, ur->req))
                {
                    struct user_answer ua;
                    ua.is_succsess = true;
                    ua.message = "add success";
                    ua.node = NULL;
                    return ua;
                }
                else
                {
                    struct user_answer ua;
                    ua.is_succsess = false;
                    ua.message = "id already exist";
                    ua.node = NULL;
                    return ua;
                }
            }

            print_debug("Добавляю всю базу данных\n");
            struct index_list* first = NULL;
            struct index_list* last = NULL;
            struct node** steck;

            size_t steck_size = 0;
            steck = (struct node**) malloc(sizeof(struct node*) * ++steck_size);
            steck[steck_size - 1] = ur->node;

            while (steck_size > 0)
            {
                struct node* current_node = steck[--steck_size];
                steck = (struct node**) realloc(steck,  sizeof(struct node*) * steck_size);

                struct request req;
                req.node_id = current_node->id;
                req.num_of_connection_names = current_node->num_of_connections;
                req.num_of_source_attributes = current_node->num_of_attributes;
                req.request_source_attributes = (struct request_attribute*) malloc(sizeof(struct request_attribute) * req.num_of_source_attributes);
                req.connection_id = (size_t*) malloc(sizeof(size_t) * req.num_of_connection_names);
                req.connection_name = (char**) malloc(sizeof(char*) * req.num_of_connection_names);
                req.connection_role = (char*) malloc(sizeof(char) * req.num_of_connection_names);

                for (size_t i = 0; i < current_node->num_of_connections; i++)
                {
                    req.connection_name[i] = current_node->connections[i].name;
                    size_t ind;
                    struct node* connected_node;
                    if (current_node->connections[i].source == current_node)
                    {
                        req.connection_role[i] = 's';
                        ind = req.connection_id[i] = current_node->connections[i].target->id;
                        connected_node = current_node->connections[i].target;
                    }
                    else
                    {
                        req.connection_role[i] = 't';
                        ind = req.connection_id[i] = current_node->connections[i].source->id;
                        connected_node = current_node->connections[i].source;
                    }

                    bool is_exist = false;
                    if (first != NULL)
                    {
                        struct index_list* il = first;
                        while (il != NULL)
                        {
                            if (il->index == ind)
                            {
                                is_exist = true;
                                break;
                            }
                            il = il->next_list;
                        }
                        if (is_exist)
                            continue;
                    }

                    if (connected_node->num_of_attributes && !is_exist)
                    {
                        steck = (struct node**) realloc(steck,  sizeof(struct node*) * ++steck_size);
                        steck[steck_size - 1] = connected_node;
                    }

                    if (first == NULL)
                    {
                        first = last = (struct index_list*) malloc(sizeof(struct index_list));
                        first->index = ind;
                        last->next_list = NULL;
                    }
                    else
                    {
                        last->next_list = (struct index_list*) malloc(sizeof(struct index_list));
                        last = last->next_list;
                        last->index = ind;
                        last->next_list = NULL;
                    }

                }

                for (size_t i = 0; i < current_node->num_of_attributes; i++)
                    req.request_source_attributes[i].attribute = current_node->attributes[i];

                add_to_db(db, &req);
                free(req.request_source_attributes);
                free(req.connection_id);
                free(req.connection_name);
                free(req.connection_role);
            }
            struct user_answer ua;
            ua.is_succsess = false;
            ua.message = "successfully added";
            ua.node = NULL;
            return ua;
        }
        case REMOVE:
        {
            print_debug("Запрос на удаление\n");
            if (ur->req)
            {
                if (delete_from_db(db, ur->req))
                {
                    struct user_answer ua;
                    ua.is_succsess = false;
                    ua.message = "successfully deleted";
                    ua.node = NULL;
                    return ua;
                }
                else
                {
                    struct user_answer ua;
                    ua.is_succsess = false;
                    ua.message = "error on delete";
                    ua.node = NULL;
                    return ua;
                }
            }
            else
            {
                struct request req;
                req.node_id = ur->node->id;
                if (delete_from_db(db, &req))
                {
                    struct user_answer ua;
                    ua.is_succsess = false;
                    ua.message = "successfully deleted";
                    ua.node = NULL;
                    return ua;
                }
                else
                {
                    struct user_answer ua;
                    ua.is_succsess = false;
                    ua.message = "error on delete";
                    ua.node = NULL;
                    return ua;
                }
            }
        }
        case UPDATE:
        {
            print_debug("Запрос на изменение\n");
            struct request req;
            req.node_id = ur->node->id;
            if (delete_from_db(db, &req))
            {
                req.num_of_connection_names = ur->node->num_of_connections;
                req.num_of_source_attributes = ur->node->num_of_attributes;
                req.request_source_attributes = (struct request_attribute*) malloc(sizeof(struct request_attribute) * req.num_of_source_attributes);
                req.connection_id = (size_t*) malloc(sizeof(size_t) * req.num_of_connection_names);
                req.connection_name = (char**) malloc(sizeof(char*) * req.num_of_connection_names);
                req.connection_role = (char*) malloc(sizeof(char) * req.num_of_connection_names);

                for (size_t i = 0; i < ur->node->num_of_connections; i++)
                {
                    req.connection_name[i] = ur->node->connections[i].name;
                    size_t ind;
                    if (ur->node->connections[i].source == ur->node)
                    {
                        req.connection_role[i] = 's';
                        req.connection_id[i] = ur->node->connections[i].target->id;
                        ind = ur->node->connections[i].target->id;
                    }
                    else
                    {
                        req.connection_role[i] = 't';
                        req.connection_id[i] = ur->node->connections[i].source->id;
                        ind = ur->node->connections[i].source->id;
                    }
                }

                for (size_t i = 0; i < ur->node->num_of_attributes; i++)
                    req.request_source_attributes[i].attribute = ur->node->attributes[i];

                add_to_db(db, &req);
                free(req.request_source_attributes);
                free(req.connection_id);
                free(req.connection_name);
                free(req.connection_role);

                struct user_answer ua;
                ua.is_succsess = false;
                ua.message = "updating sucseed";
                ua.node = NULL;
                return ua;
            }
            else
            {
                struct user_answer ua;
                ua.is_succsess = false;
                ua.message = "error on updating";
                ua.node = NULL;
                return ua;
            }
        }
        case GET:
        {
            print_debug("Получить запись\n");
            struct get_result result = get_from_db(db, ur->req);
            if (result.nodes == NULL || result.size == 0)
            {
                struct user_answer ua;
                ua.is_succsess = false;
                ua.message = "error on getting";
                ua.node = NULL;
                return ua;
            }
            else
            {
                struct node* nd = (struct node*) malloc(sizeof(struct node));
                nd->id = result.nodes->node_id;
                nd->num_of_attributes = result.nodes->num_attributes;
                nd->num_of_connections = result.nodes->num_of_connections;
                nd->attributes = (struct attribute*) malloc(sizeof(struct attribute) * nd->num_of_attributes);
                for (size_t i = 0; i < nd->num_of_attributes; i++)
                    nd->attributes[i] = result.nodes->attributes[i];
                nd->connections = (struct connection*) malloc(sizeof(struct connection) * nd->num_of_connections);
                for (size_t i = 0; i < nd->num_of_connections; i++)
                {
                    struct connection* con_tmp = malloc(sizeof(struct connection));
                    con_tmp->name = result.nodes->connection_names[i];
                    if (result.nodes->connection_roles[i] == 's')
                    {
                        con_tmp->source = nd;
                        struct node* small_nd = (struct node*) malloc(sizeof(struct node));
                        small_nd->id = result.nodes->connection_ids[i];
                        small_nd->num_of_attributes = 0;
                        small_nd->num_of_connections = 0;
                        con_tmp->target = small_nd;
                    }
                    nd->connections[i] = *con_tmp;
                }
                struct user_answer ua;
                ua.is_succsess = true;
                ua.message = "geted";
                ua.node = nd;
                return ua;
            }
        }
    }
}
