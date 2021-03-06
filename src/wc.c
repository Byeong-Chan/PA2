#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/queue.h>

static LIST_HEAD(listhead, entry) head[5000011];

struct listhead *headp = NULL;
int num_entries = 0;

pthread_t thread[41];
pthread_mutex_t mutex[5000011];

struct entry {
    char* name;
    int frequency;
    LIST_ENTRY(entry) entries;
};

int hash(char *k) {
    int val = 0;
    while(*k != '\0') {
        val = ((val << 8) + val + *k) % 5000011;
        k++;
    }
    return val;
}

struct entry **heap[32768];
int heap_size[32768];
int heap_capacity[32768];

void push_heap(struct entry *in, int num) {
    if(heap_capacity[num] == 0) {
        heap[num] = (struct entry **)malloc(1 * sizeof(struct entry *));
        heap_capacity[num] = 1;
    }
    if(heap_capacity[num] == heap_size[num]) {
        heap_capacity[num] <<= 1;
        struct entry **s = (struct entry **)malloc(heap_capacity[num] * sizeof(struct entry *));
        for(int i = 0; i < heap_size[num]; i++) s[i] = heap[num][i];
        free(heap[num]);
        heap[num] = s;
    }
    int cur = heap_size[num]++;
    heap[num][cur] = in;
    while(cur > 0) {
        int par = (cur + 1) / 2 - 1;
        if(heap[num][par]->frequency < heap[num][cur]->frequency) {
            struct entry *swp = heap[num][par];
            heap[num][par] = heap[num][cur];
            heap[num][cur] = swp;
        }
        else if(heap[num][par]->frequency == heap[num][cur]->frequency && strcmp(heap[num][par]->name, heap[num][cur]->name) > 0) {
            struct entry *swp = heap[num][par];
            heap[num][par] = heap[num][cur];
            heap[num][cur] = swp;
        }
        else break;
        
        cur = par;
    }
}

struct entry *pop_heap(int num) {
    struct entry *tmp = heap[num][0];
    int cur = --heap_size[num];
    heap[num][0] = heap[num][cur];
    heap[num][cur] = NULL;
    cur = 0;
    while(1) {
        int l = (cur + 1) * 2 - 1;
        int r = l + 1;
        if(l >= heap_size[num]) break;
        if(r >= heap_size[num]) {
            if(heap[num][l]->frequency > heap[num][cur]->frequency) {
                struct entry *swp = heap[num][l];
                heap[num][l] = heap[num][cur];
                heap[num][cur] = swp;
                
                cur = l;
            }
            else if(heap[num][l]->frequency == heap[num][cur]->frequency && strcmp(heap[num][l]->name, heap[num][cur]->name) < 0) {
                struct entry *swp = heap[num][l];
                heap[num][l] = heap[num][cur];
                heap[num][cur] = swp;
                
                cur = l;
            }
            else break;
            continue;
        }
        if(heap[num][l]->frequency > heap[num][r]->frequency) {
            if(heap[num][l]->frequency > heap[num][cur]->frequency) {
                struct entry *swp = heap[num][l];
                heap[num][l] = heap[num][cur];
                heap[num][cur] = swp;
                
                cur = l;
            }
            else if(heap[num][l]->frequency == heap[num][cur]->frequency && strcmp(heap[num][l]->name, heap[num][cur]->name) < 0) {
                struct entry *swp = heap[num][l];
                heap[num][l] = heap[num][cur];
                heap[num][cur] = swp;
                
                cur = l;
            }
            else break;
        }
        else if(heap[num][l]->frequency == heap[num][r]->frequency && strcmp(heap[num][l]->name, heap[num][r]->name) < 0) {
            if(heap[num][l]->frequency > heap[num][cur]->frequency) {
                struct entry *swp = heap[num][l];
                heap[num][l] = heap[num][cur];
                heap[num][cur] = swp;
                
                cur = l;
            }
            else if(heap[num][l]->frequency == heap[num][cur]->frequency && strcmp(heap[num][l]->name, heap[num][cur]->name) < 0) {
                struct entry *swp = heap[num][l];
                heap[num][l] = heap[num][cur];
                heap[num][cur] = swp;
                
                cur = l;
            }
            else break;
        }
        else {
            if(heap[num][r]->frequency > heap[num][cur]->frequency) {
                struct entry *swp = heap[num][r];
                heap[num][r] = heap[num][cur];
                heap[num][cur] = swp;
                
                cur = r;
            }
            else if(heap[num][r]->frequency == heap[num][cur]->frequency && strcmp(heap[num][r]->name, heap[num][cur]->name) < 0) {
                struct entry *swp = heap[num][r];
                heap[num][r] = heap[num][cur];
                heap[num][cur] = swp;
                
                cur = r;
            }
            else break;
        }
    }
    return tmp;
}

void *listing(void *args) {
  char **inp = (char **)args;
  for(int i = 0; i < 1024; i++) {
    if(inp[i] == NULL) break;
    char *buf = inp[i];

    int line_length = strlen(buf);
        
    // Tokenization
    char *tok = NULL;
    const char* WHITE_SPACE =" \t\n";

    for(int i = line_length - 1; i >= 0; i--) {
      for(int j = 0; j < 3; j++) {
        if(WHITE_SPACE[j] == buf[i]) buf[i] = '\0';
      }
      if(buf[i] != '\0') tok = buf + i;
    }

    if(tok == NULL) continue;
        
    do {
      int cur = hash(tok);
      pthread_mutex_lock(&mutex[cur]);
      int flag = 0;
      struct entry *last = NULL;
      for(struct entry *np = head[cur].lh_first; np != NULL; np = np->entries.le_next) {
        last = np;
        int judge = strcmp(np->name, tok);
        if(judge == 0) {
          np->frequency++;
          flag = 1;
          break;
        }
        else if(judge > 0) {
          struct entry* e = malloc(sizeof(struct entry));
                    
          e->name = (char *)malloc(sizeof(char) * (strlen(tok) + 1));
          strcpy(e->name, tok);
          e->frequency = 1;
                    
          LIST_INSERT_BEFORE(np, e, entries);
          flag = 1;
          break;
        }
      }
      if(!flag) {
        struct entry* e = malloc(sizeof(struct entry));
                
        e->name = (char *)malloc(sizeof(char) * (strlen(tok) + 1));
        strcpy(e->name, tok);
        e->frequency = 1;
                
        if(last == NULL) LIST_INSERT_HEAD(&head[cur], e, entries);
        else LIST_INSERT_AFTER(last, e, entries);
      }
      pthread_mutex_unlock(&mutex[cur]);
      int sw = 0;
      while(tok - buf < line_length) {
        if(!sw && *tok == '\0') sw = 1;
        if(sw && *tok != '\0') break;
        tok++;
      }
      if(tok - buf == line_length) tok = NULL;
    } while (tok);

    free(inp[i]);
  }
  free(inp);
  return NULL;
}

int main(int argc, char** argv)
{
  //time_t begin = clock();
  if (argc != 2) {
      fprintf(stderr, "%s: not enough input\n", argv[0]);
      exit(1);
  }
    
  FILE* fp = fopen(argv[1], "r");
  char buf[4096];
    
  for(int i = 0; i < 5000011; i++) {
    LIST_INIT(&head[i]);
    pthread_mutex_init(&mutex[i], NULL);
  }
  int count = 0;
  int size = -1;
  int joined = 0;
  char **inp = (char **)calloc(1024, sizeof(char *));
  while (fgets(buf, 4096, fp)) {
    size++;
    if(size == 1024) {
      if(joined) {
        void *sh;
        pthread_join(thread[count], &sh);
      }
      pthread_create(&thread[count], NULL, listing, (void *)inp);

      count++;
      size = 0;
          
      if(count == 40) {
        count = 0;
        joined = 1;
      }
      inp = (char **)calloc(1024, sizeof(char *));
    }

    // Preprocess: change all non-alnums into white-space,
    //             alnums to lower-case.
    int line_length = strlen(buf);
        
    for (int it = 0; it < line_length; ++it) {
      if (!isalnum(buf[it])) {
        buf[it] = ' ';
      } else {
        buf[it] = tolower(buf[it]);
      }
    }

    inp[size] = (char *)calloc(4096, sizeof(char));
    strcpy(inp[size], buf);
  }
  if(joined) {
    void *sh;
    pthread_join(thread[count], &sh);
  }
  pthread_create(&thread[count], NULL, listing, (void *)inp);
  
  if(joined) {
    for(int i = 0; i < 40; i++) {
      void *sh;
      pthread_join(thread[i], &sh);
    }
  }
  else {
    for(int i = 0; i <= count; i++) {
      void *sh;
      pthread_join(thread[i], &sh);
    }
  }
    
  for(int i = 0; i < 5000011; i++) {
    for(struct entry *np = head[i].lh_first; np != NULL; np = np->entries.le_next) {
      int x = np->frequency;
      int d[3] = { 0, 0, 0, };
      int len = 0;
      while(x) {
        d[2] = d[1];
        d[1] = d[0];
        d[0] = x % 10;
        x /= 10;
        len++;
      }
      push_heap(np, len * 1000 + d[0] * 100 + d[1] * 10 + d[2]);
    }
  }
    
  for(int i = 32767; i >= 0; i--) {
    while(heap_size[i] > 0) {
      struct entry *np = pop_heap(i);
      printf("%s %d\n", np->name, np->frequency);
    }
  }
    
  // Print the counting result very very slow way.
  /*int max_frequency = 0;
     
  for (struct entry* np = head.lh_first; np != NULL; np = np->entries.le_next) {
      if (max_frequency < np->frequency) {
      max_frequency = np->frequency;
    }
  }
     
  for (int it = max_frequency; it > 0; --it) {
    for (struct entry* np = head.lh_first; np != NULL; np = np->entries.le_next) {
      if (np->frequency == it) {
        printf("%s %d\n", np->name, np->frequency);
      }
    }
  }*/
    
  // Release
  for(int i = 0; i < 5000011; i++) {
    pthread_mutex_destroy(&mutex[i]);
    while (head[i].lh_first != NULL) {
      struct entry * np = head[i].lh_first;
      free(np->name);
      LIST_REMOVE(np, entries);
      free(np);
    }
  }
  for(int i = 0; i < 32768; i++) {
    if(heap_capacity[i] == 0) continue;
    free(heap[i]);
  }
    
  fclose(fp);
  //time_t end = clock();
  //printf("%lu\n", end - begin);
  return 0;
}
