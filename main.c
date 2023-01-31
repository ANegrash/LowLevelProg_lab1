#include <stdio.h>
#include "graph/graph.h"
#include "time.h"
#include "stdlib.h"

#define TEST_ADD
#define NUM_OF_ADD 20000
#define TEST_READ
#define NUM_OF_READ 20000
#define TEST_DELETE
#define NUM_OF_DELETE 10000

int main() {
    struct user_request req;
    req.act = ADD;
    struct node node1;
    node1.num_of_attributes = 1;
    node1.num_of_connections = 0;
    node1.attributes = malloc(sizeof(struct attribute) * 1);
    struct attribute atr1;
    atr1.name = "atr1";
    atr1.type = INT32;
    atr1.value.int32 = 2;
    node1.attributes[0] = atr1;
    req.node = &node1;
    req.req = NULL;

    struct user_request req1;
    req1.act = GET;
    req1.req = malloc(sizeof(struct request));

    struct user_request req2;
    req2.act = REMOVE;
    req2.req = malloc(sizeof(struct request));

    FILE* fl;
    struct database_struct* ds;
    size_t id;

#ifdef TEST_ADD
    fl = fopen("times_add.txt", "w");
    ds = open_database("graph_add.db");
    id = 1;
    for (int i = 0; i < NUM_OF_ADD; i++)
    {
        node1.id = id;

        clock_t start_time = clock();
        do_request(ds, &req);
        clock_t end_time = clock();

        fprintf(fl, "%lli\n", (long long)(end_time - start_time));
        printf("Add: %d/%d, time: %d\n", i, NUM_OF_ADD, (long long)(end_time - start_time));
        id++;
    }
    fclose(fl);
    close_database(ds);
#endif

#ifdef TEST_READ
    id = 1;
    fl = fopen("times_read.txt", "w");
    ds = open_database("graph_read.db");
    for (int i = 0; i < NUM_OF_READ; i++)
    {
        node1.id = id;

        do_request(ds, &req);

        size_t rand_id = (rand() % (id)) + 1;
        req1.req->node_id = rand_id;
        clock_t start_time = clock();
        struct user_answer ua = do_request(ds, &req1);
        clock_t end_time = clock();

        printf("Read: %d/%d, time: %d, random id: %d, attribute name: %s, attribute value: %d\n", i, NUM_OF_READ, (long long)(end_time - start_time), rand_id, ua.node->attributes[0].name, ua.node->attributes[0].value.int32);
        fprintf(fl, "%lli, %lli\n", (long long)(end_time - start_time), rand_id);
        id++;
        free(ua.node->attributes[0].name);
        free(ua.node->attributes);
        free(ua.node);
    }
    fclose(fl);
    close_database(ds);
#endif

#ifdef TEST_DELETE
    fl = fopen("times_delete.txt", "w");
    ds = open_database("graph_delete.db");
    id = 1;
    for (int i = 0; i < NUM_OF_DELETE * 2; i++)
    {
        node1.id = id;
        do_request(ds, &req);
        printf("Add before delete: %d\n", i);
        id++;
    }
    for (int i = 0; i < NUM_OF_DELETE; i++)
    {
        size_t rand_id = (rand() % NUM_OF_DELETE) + 1;
        req2.req->node_id = rand_id;

        clock_t start_time = clock();
        do_request(ds, &req2);
        clock_t end_time = clock();

        printf("Delete: %d/%d, time: %d, random id: %d\n", i, NUM_OF_DELETE, (long long)(end_time - start_time), rand_id);
        fprintf(fl, "%lli, %lli\n", (long long)(end_time - start_time), rand_id);
        id++;
    }
    fclose(fl);
    close_database(ds);
#endif

    printf("Tests complete\n");
}
