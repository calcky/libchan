/*
 * snake_chan.c — Terminal Snake with an input channel
 *
 *   [input thread]  WASD / arrows  ──► [dir channel] ──► [game loop] draw + tick
 *
 * Build: cmake --build build --target snake_chan
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <termios.h>
#include <pthread.h>
#include <time.h>

#include "libchan.h"

#define W       44
#define H       24
#define TICK_MS 140
#define MAXLEN  (W * H)
#define DRAWBUF (W * H + 256)

typedef enum { DIR_UP, DIR_DOWN, DIR_LEFT, DIR_RIGHT, DIR_NONE } dir_t;

typedef struct { int x, y; } pos_t;

static struct termios g_orig_term;
static int g_alt_screen;

static void term_setup(void) {
    tcgetattr(STDIN_FILENO, &g_orig_term);
    struct termios raw = g_orig_term;
    raw.c_iflag &= (tcflag_t)~(BRKINT | ICRNL | INPCK | ISTRIP | IXON);
    /* Keep OPOST: without it \\n does not return the carriage, rows overlap. */
    raw.c_cflag |= (tcflag_t)CS8;
    raw.c_lflag &= (tcflag_t)~(ECHO | ICANON | IEXTEN | ISIG);
    raw.c_cc[VMIN]  = 0;
    raw.c_cc[VTIME] = 0;
    tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw);
    setvbuf(stdin, NULL, _IONBF, 0);
    setvbuf(stdout, NULL, _IONBF, 0);
    tcflush(STDIN_FILENO, TCIFLUSH);
}

static void term_enter_alt(void) {
    fputs("\033[?1049h\033[?25l\033[H", stdout);
    g_alt_screen = 1;
}

static void term_leave_alt(void) {
    if (!g_alt_screen) return;
    fputs("\033[?1049l\033[?25h", stdout);
    g_alt_screen = 0;
}

static void term_restore(void) {
    term_leave_alt();
    tcsetattr(STDIN_FILENO, TCSAFLUSH, &g_orig_term);
}

static int read_byte(void) {
    unsigned char c;
    return read(STDIN_FILENO, &c, 1) == 1 ? (int)c : -1;
}

static dir_t read_escape_seq(void) {
    int c = read_byte();
    if (c < 0) return DIR_NONE;

    if (c == 'O') {
        int f = read_byte();
        if (f < 0) return DIR_NONE;
        switch (f) {
        case 'A': return DIR_UP;
        case 'B': return DIR_DOWN;
        case 'C': return DIR_RIGHT;
        case 'D': return DIR_LEFT;
        default:  return DIR_NONE;
        }
    }

    if (c == '[') {
        for (;;) {
            int f = read_byte();
            if (f < 0) return DIR_NONE;
            if (f >= 'A' && f <= '~') {
                switch (f) {
                case 'A': return DIR_UP;
                case 'B': return DIR_DOWN;
                case 'C': return DIR_RIGHT;
                case 'D': return DIR_LEFT;
                default:  return DIR_NONE;
                }
            }
        }
    }
    return DIR_NONE;
}

static dir_t key_to_dir(int c) {
    switch (c) {
    case 'w': case 'W': return DIR_UP;
    case 's': case 'S': return DIR_DOWN;
    case 'a':           return DIR_LEFT;
    case 'd': case 'D': return DIR_RIGHT;
    default:            return DIR_NONE;
    }
}

static dir_t opposite(dir_t a, dir_t b) {
    return (a == DIR_UP    && b == DIR_DOWN)  ||
           (a == DIR_DOWN  && b == DIR_UP)    ||
           (a == DIR_LEFT  && b == DIR_RIGHT) ||
           (a == DIR_RIGHT && b == DIR_LEFT);
}

static void send_dir(chan_t *dir_ch, dir_t d) {
    while (chan_send(dir_ch, &d) != CHAN_OK) {
        if (chan_is_closed(dir_ch)) return;
    }
}

static void drain_dirs(chan_t *dir_ch) {
    dir_t junk;
    while (chan_try_recv(dir_ch, &junk) == CHAN_OK)
        ;
}

static void *input_thread(void *arg) {
    chan_t *dir_ch = arg;
    for (;;) {
        int c = read_byte();
        if (c < 0) {
            struct timespec ts = { 0, 5 * 1000000L };
            nanosleep(&ts, NULL);
            continue;
        }
        if (c == 'q' || c == 'Q') {
            chan_close(dir_ch);
            break;
        }
        dir_t d = key_to_dir(c);
        if (d == DIR_NONE && c == 27)
            d = read_escape_seq();
        if (d != DIR_NONE)
            send_dir(dir_ch, d);
    }
    return NULL;
}

static void apply_dirs(chan_t *dir_ch, dir_t *cur) {
    dir_t d;
    while (chan_try_recv(dir_ch, &d) == CHAN_OK) {
        if (!opposite(*cur, d))
            *cur = d;
    }
}

static void place_food(const pos_t *snake, int len, pos_t *food) {
    for (int tries = 0; tries < 1000; tries++) {
        food->x = 1 + rand() % (W - 2);
        food->y = 1 + rand() % (H - 2);
        int on_snake = 0;
        for (int i = 0; i < len; i++) {
            if (snake[i].x == food->x && snake[i].y == food->y) {
                on_snake = 1;
                break;
            }
        }
        if (!on_snake) return;
    }
}

static int hit_wall(pos_t p) {
    return p.x <= 0 || p.x >= W - 1 || p.y <= 0 || p.y >= H - 1;
}

static int hit_self(const pos_t *snake, int len, pos_t head) {
    for (int i = 1; i < len; i++)
        if (snake[i].x == head.x && snake[i].y == head.y)
            return 1;
    return 0;
}

static void draw_frame(const pos_t *snake, int len, pos_t food, int score, dir_t cur,
                       pos_t crash, int crashed, const char *footer) {
    char buf[DRAWBUF];
    size_t n = 0;

    n += (size_t)snprintf(buf + n, sizeof(buf) - n,
                          "\033[Hsnake_chan  score %d  WASD/arrows  q=quit  dir: ",
                          score);
    switch (cur) {
    case DIR_UP:    n += (size_t)snprintf(buf + n, sizeof(buf) - n, "UP");    break;
    case DIR_DOWN:  n += (size_t)snprintf(buf + n, sizeof(buf) - n, "DOWN");  break;
    case DIR_LEFT:  n += (size_t)snprintf(buf + n, sizeof(buf) - n, "LEFT");  break;
    case DIR_RIGHT: n += (size_t)snprintf(buf + n, sizeof(buf) - n, "RIGHT"); break;
    default:        n += (size_t)snprintf(buf + n, sizeof(buf) - n, "-");     break;
    }
    n += (size_t)snprintf(buf + n, sizeof(buf) - n, "\n");

    for (int y = 0; y < H; y++) {
        for (int x = 0; x < W; x++) {
            char ch = ' ';
            if (x == 0 || x == W - 1 || y == 0 || y == H - 1)
                ch = '#';
            else if (crashed && x == crash.x && y == crash.y)
                ch = 'X';
            else if (x == food.x && y == food.y)
                ch = '*';
            else {
                for (int i = 0; i < len; i++) {
                    if (snake[i].x == x && snake[i].y == y) {
                        ch = (i == 0 && !crashed) ? '@' : 'o';
                        break;
                    }
                }
            }
            if (n + 1 >= sizeof(buf)) break;
            buf[n++] = ch;
        }
        if (n + 1 >= sizeof(buf)) break;
        buf[n++] = '\n';
    }
    if (footer && n < sizeof(buf))
        n += (size_t)snprintf(buf + n, sizeof(buf) - n, "\n%s", footer);
    buf[n < sizeof(buf) ? n : sizeof(buf) - 1] = '\0';
    fputs(buf, stdout);
}

static void draw(const pos_t *snake, int len, pos_t food, int score, dir_t cur) {
    draw_frame(snake, len, food, score, cur, (pos_t){0, 0}, 0, NULL);
}

static void wait_any_key(void) {
    fputs("Press any key to exit...", stdout);
    fflush(stdout);
    for (;;) {
        int c = read_byte();
        if (c >= 0) return;
        struct timespec ts = { 0, 20 * 1000000L };
        nanosleep(&ts, NULL);
    }
}

static void tick_sleep(chan_t *dir_ch, dir_t *cur, int ms) {
    for (int elapsed = 0; elapsed < ms; elapsed += 5) {
        apply_dirs(dir_ch, cur);
        if (chan_is_closed(dir_ch)) return;
        struct timespec ts = { 0, 5 * 1000000L };
        nanosleep(&ts, NULL);
    }
}

int main(void) {
    chan_t *dir_ch = chan_create(sizeof(dir_t), 4);

    term_setup();
    atexit(term_restore);
    term_enter_alt();

    pthread_t it;
    pthread_create(&it, NULL, input_thread, dir_ch);

    pos_t snake[MAXLEN];
    int len = 3;
    int mid = W / 2;
    int midy = H / 2;
    snake[0] = (pos_t){ mid,     midy };
    snake[1] = (pos_t){ mid - 1, midy };
    snake[2] = (pos_t){ mid - 2, midy };

    dir_t cur = DIR_RIGHT;
    pos_t food;
    int score = 0;
    srand((unsigned)time(NULL));
    place_food(snake, len, &food);

    /* Drop focus-report / arrow garbage before gameplay. */
    struct timespec warm = { 0, 200 * 1000000L };
    nanosleep(&warm, NULL);
    drain_dirs(dir_ch);
    tcflush(STDIN_FILENO, TCIFLUSH);

    draw(snake, len, food, score, cur);

    int game_over = 0;
    for (;;) {
        apply_dirs(dir_ch, &cur);
        if (chan_is_closed(dir_ch))
            break;

        pos_t head = snake[0];
        switch (cur) {
        case DIR_UP:    head.y--; break;
        case DIR_DOWN:  head.y++; break;
        case DIR_LEFT:  head.x--; break;
        case DIR_RIGHT: head.x++; break;
        default: break;
        }

        if (hit_wall(head) || hit_self(snake, len, head)) {
            game_over = 1;
            char footer[64];
            snprintf(footer, sizeof(footer), "*** GAME OVER — score %d ***", score);
            draw_frame(snake, len, food, score, cur, head, 1, footer);
            wait_any_key();
            break;
        }

        int grow = (head.x == food.x && head.y == food.y);
        if (grow) {
            score++;
            if (len < MAXLEN)
                len++;
            place_food(snake, len, &food);
        }

        memmove(snake + 1, snake, (size_t)(len - 1) * sizeof(pos_t));
        snake[0] = head;

        draw(snake, len, food, score, cur);
        tick_sleep(dir_ch, &cur, TICK_MS);
    }

    term_restore();
    if (!game_over)
        printf("Quit — score %d\n", score);

    chan_close(dir_ch);
    pthread_join(it, NULL);
    chan_destroy(dir_ch);
    return 0;
}
