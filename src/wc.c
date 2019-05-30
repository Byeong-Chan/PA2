#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/queue.h>

static LIST_HEAD(listhead, entry) head[100019];

int check_tid[100019];
pthread_t hash_tid[100019];

struct listhead *headp = NULL;
int num_entries = 0;

struct entry {
	char name[BUFSIZ];
	int frequency;
	LIST_ENTRY(entry) entries;
};

int hash(char *k) {
  int val = 0;
  while(*k != '\0') {
    val = ((val << 8) + val + *k) % 100019;
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

void *thread_hash(void *arg) {
  struct entry *p = (struct entry *)arg;
  int cur = hash(p->name);

  int flag = 0;
  for(struct entry *np = head[cur].lh_first; np != NULL; np = np->entries.le_next) {
    if(strcmp(np->name, p->name) == 0) {
      np->frequency++;
      flag = 1;
      break;
    }
  }
  if(!flag) {
    LIST_INSERT_HEAD(&head[cur], p, entries);
  }
  return (void *)NULL;
}

int main(int argc, char** argv)
{
	if (argc != 2) {
		fprintf(stderr, "%s: not enough input\n", argv[0]);
		exit(1);
	}

	FILE* fp = fopen(argv[1], "r");
	char buf[4096];

  for(int i = 0; i < 100019; i++) {
  	LIST_INIT(&head[i]);
  }

	while (fgets(buf, 4096, fp)) {
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

    // Tokenization
		const char* WHITE_SPACE =" \t\n";
		char* tok = strtok(buf, WHITE_SPACE);

		if (tok == NULL || strcmp(tok, "") == 0) {
			continue;
		}

		do {
      struct entry* e = malloc(sizeof(struct entry));

      int cur = hash(tok);
      strcpy(e->name, tok);
      e->frequency = 1;

      if(check_tid[cur] == 1) {
        void *sh;
        pthread_join(hash_tid[cur], sh);
      }
      check_tid[cur] = 1;
      pthread_create(&hash_tid[cur], NULL, thread_hash, (void *)e);
    } while (tok = strtok(NULL, WHITE_SPACE));
  }

  for(int i = 0; i < 100019; i++) {
    if(check_tid[i] == 1) {
      void *sh;
      pthread_join(hash_tid[i], sh);
    }
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
  for(int i = 0; i < 100019; i++) {
    while (head[i].lh_first != NULL) {
      LIST_REMOVE(head[i].lh_first, entries);
    }
  }
  for(int i = 0; i < 32768; i++) {
    if(heap_capacity[i] == 0) continue;
    free(heap[i]);
  }

  fclose(fp);

  return 0;
}
