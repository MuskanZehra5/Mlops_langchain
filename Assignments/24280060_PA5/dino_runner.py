import pygame
import random
import pickle
import os

# Constants
SCREEN_WIDTH = 1100
SCREEN_HEIGHT = 600
GROUND_HEIGHT = 50
GRASS_HEIGHT = 15
FPS = 60

# Colors
SKY_DAY = (135, 206, 235)
SKY_NIGHT = (44, 22, 84)
GROUND_COLOR = (139, 69, 19)
GRASS_COLOR = (34, 139, 34)
DINO_BODY = (46, 204, 113)
DINO_LEGS = (39, 174, 96)
DINO_EYE = (255, 255, 255)
DINO_PUPIL = (0, 0, 0)
DINO_DUCK = (149, 165, 166)
CACTUS_COLOR = (26, 92, 26)
PTERODACTYL_COLOR = (230, 126, 34)
CLOUD_COLOR = (255, 255, 255)
SCORE_COLOR = (0, 0, 0)
HIGH_SCORE_COLOR = (255, 215, 0)
GAME_OVER_COLOR = (220, 20, 60)

# Game Settings
DAY_NIGHT_START = 500
DAY_NIGHT_END = 1000
SPEED_INCREMENT = 0.05
MAX_SPEED = 15.0
GRAVITY = 0.8
JUMP_STRENGTH = -15
DINO_WIDTH = 44
DINO_HEIGHT = 47
DINO_DUCK_HEIGHT = 25
DINO_Y = SCREEN_HEIGHT - GROUND_HEIGHT - DINO_HEIGHT
DINO_DUCK_Y = SCREEN_HEIGHT - GROUND_HEIGHT - DINO_DUCK_HEIGHT
CLOUD_COUNT = 5
CLOUD_SPEED = 0.2
CLOUD_MIN_Y = 50
CLOUD_MAX_Y = 200
CLOUD_MIN_SIZE = 30
CLOUD_MAX_SIZE = 80
OBSTACLE_MIN_GAP = 500
OBSTACLE_MAX_GAP = 1500
CACTUS_TYPES = [1, 2, 3]
PTERODACTYL_HEIGHTS = [DINO_Y - 80, DINO_Y - 120, DINO_Y - 160]
PTERODACTYL_FLAP_SPEED = 10

# High Score File
HIGH_SCORE_FILE = "highscore.dat"

# Helper functions
def load_high_score():
    if os.path.exists(HIGH_SCORE_FILE):
        try:
            with open(HIGH_SCORE_FILE, 'rb') as f:
                return pickle.load(f)
        except:
            return 0
    return 0

def save_high_score(score):
    with open(HIGH_SCORE_FILE, 'wb') as f:
        pickle.dump(score, f)

# Dino class
class Dino:
    def __init__(self):
        self.width = DINO_WIDTH
        self.height = DINO_HEIGHT
        self.y = DINO_Y
        self.ducking = False
        self.jumping = False
        self.jump_velocity = 0
        self.jumped_twice = False
        self.animation_timer = 0
        self.animation_frame = 0
        self.rect = pygame.Rect(50, self.y, self.width, self.height)
        self.eye_offset = 10

    def update(self):
        if self.ducking and not self.jumping:
            self.height = DINO_DUCK_HEIGHT
            self.y = DINO_DUCK_Y
        elif not self.jumping:
            self.height = DINO_HEIGHT
            self.y = DINO_Y

        if self.jumping:
            self.y += self.jump_velocity
            self.jump_velocity += GRAVITY
            if self.y >= DINO_Y:
                self.y = DINO_Y
                self.jumping = False
                self.jump_velocity = 0
                self.jumped_twice = False

        self.animation_timer += 1
        if self.animation_timer >= 10:
            self.animation_frame = (self.animation_frame + 1) % 2
            self.animation_timer = 0

        self.rect = pygame.Rect(50, self.y, self.width, self.height)

    def jump(self):
        if not self.jumping:
            self.jumping = True
            self.jump_velocity = JUMP_STRENGTH
        elif not self.jumped_twice:
            self.jumped_twice = True
            self.jumping = True
            self.jump_velocity = JUMP_STRENGTH

    def duck(self):
        self.ducking = True

    def unduck(self):
        self.ducking = False

    def draw(self, screen):
        # Body
        body_rect = pygame.Rect(50, self.y, self.width, self.height)
        if self.ducking:
            pygame.draw.rect(screen, DINO_DUCK, body_rect)
            # Ducking legs
            pygame.draw.rect(screen, DINO_LEGS, (50 + 5, self.y + self.height - 8, 10, 8))
            pygame.draw.rect(screen, DINO_LEGS, (50 + self.width - 15, self.y + self.height - 8, 10, 8))
        else:
            pygame.draw.rect(screen, DINO_BODY, body_rect)
            # Legs animation
            leg_offset = 5 if self.animation_frame == 0 else 10
            pygame.draw.rect(screen, DINO_LEGS, (50 + 5, self.y + self.height - 8, 10, 8))
            pygame.draw.rect(screen, DINO_LEGS, (50 + self.width - 15, self.y + self.height - 8 + leg_offset, 10, 8))
        
        # Eyes
        eye_x = 50 + self.width - self.eye_offset - 5
        eye_y = self.y + 10
        pygame.draw.circle(screen, DINO_EYE, (eye_x, eye_y), 5)
        pygame.draw.circle(screen, DINO_PUPIL, (eye_x + 1, eye_y + 1), 2)

# Cactus class
class Cactus:
    def __init__(self, type_num, speed):
        self.speed = speed
        self.type = type_num
        if type_num == 1:
            self.width = 20
            self.height = 50
        elif type_num == 2:
            self.width = 30
            self.height = 70
        else:  # type 3
            self.width = 25
            self.height = 60
        self.x = SCREEN_WIDTH
        self.y = SCREEN_HEIGHT - GROUND_HEIGHT - self.height
        self.rect = pygame.Rect(self.x, self.y, self.width, self.height)

    def update(self):
        self.x -= self.speed
        self.rect.x = self.x

    def draw(self, screen):
        pygame.draw.rect(screen, CACTUS_COLOR, self.rect)
        if self.type == 1:
            # Simple cactus
            pass
        elif self.type == 2:
            # Tall cactus with spikes
            pygame.draw.rect(screen, CACTUS_COLOR, (self.x + 5, self.y - 10, 20, 10))
        else:  # type 3
            # Clustered cactus
            pygame.draw.rect(screen, CACTUS_COLOR, (self.x - 5, self.y - 15, 10, 15))

# Pterodactyl class
class Pterodactyl:
    def __init__(self, height, speed):
        self.speed = speed
        self.height = height
        self.width = 50
        self.x = SCREEN_WIDTH
        self.y = height
        self.flap_timer = 0
        self.flap_frame = 0
        self.rect = pygame.Rect(self.x, self.y, self.width, 20)

    def update(self):
        self.x -= self.speed
        self.flap_timer += 1
        if self.flap_timer >= PTERODACTYL_FLAP_SPEED:
            self.flap_frame = (self.flap_frame + 1) % 2
            self.flap_timer = 0
        self.rect.x = self.x
        self.rect.y = self.y

    def draw(self, screen):
        # Body
        pygame.draw.rect(screen, PTERODACTYL_COLOR, (self.x + 10, self.y, 30, 10))
        # Wings
        if self.flap_frame == 0:
            pygame.draw.polygon(screen, PTERODACTYL_COLOR, [(self.x, self.y + 5), (self.x + 15, self.y - 10), (self.x + 15, self.y + 15)])
            pygame.draw.polygon(screen, PTERODACTYL_COLOR, [(self.x + 40, self.y + 5), (self.x + 50, self.y - 10), (self.x + 50, self.y + 15)])
        else:
            pygame.draw.polygon(screen, PTERODACTYL_COLOR, [(self.x, self.y + 5), (self.x + 15, self.y - 5), (self.x + 15, self.y + 10)])
            pygame.draw.polygon(screen, PTERODACTYL_COLOR, [(self.x + 40, self.y + 5), (self.x + 50, self.y - 5), (self.x + 50, self.y + 10)])

# Cloud class
class Cloud:
    def __init__(self):
        self.size = random.randint(CLOUD_MIN_SIZE, CLOUD_MAX_SIZE)
        self.x = random.randint(0, SCREEN_WIDTH)
        self.y = random.randint(CLOUD_MIN_Y, CLOUD_MAX_Y)
        self.speed = CLOUD_SPEED

    def update(self):
        self.x -= self.speed
        if self.x < -self.size:
            self.x = SCREEN_WIDTH + self.size
            self.y = random.randint(CLOUD_MIN_Y, CLOUD_MAX_Y)
            self.size = random.randint(CLOUD_MIN_SIZE, CLOUD_MAX_SIZE)

    def draw(self, screen):
        pygame.draw.ellipse(screen, CLOUD_COLOR, (self.x, self.y, self.size, self.size // 2))
        pygame.draw.ellipse(screen, CLOUD_COLOR, (self.x + self.size // 3, self.y - self.size // 6, self.size // 2, self.size // 3))
        pygame.draw.ellipse(screen, CLOUD_COLOR, (self.x + self.size // 2, self.y, self.size // 2, self.size // 2))

# Game class
class Game:
    def __init__(self):
        if os.getenv('DISPLAY') is None:
            pygame.init()
            self.screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT), pygame.NOFRAME)
        else:
            pygame.init()
            self.screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT))
        pygame.display.set_caption("Chrome Dino Runner")
        self.clock = pygame.time.Clock()
        self.font = pygame.font.SysFont('Arial', 30)
        self.high_score = load_high_score()
        self.reset()

    def reset(self):
        self.dino = Dino()
        self.obstacles = []
        self.clouds = [Cloud() for _ in range(CLOUD_COUNT)]
        self.score = 0
        self.game_speed = 8.0
        self.game_over = False
        self.day_night_factor = 0.0  # 0.0 = day, 1.0 = night

    def spawn_obstacle(self):
        if random.random() < 0.7:  # 70% chance for cactus
            cactus_type = random.choice(CACTUS_TYPES)
            self.obstacles.append(Cactus(cactus_type, self.game_speed))
        else:  # 30% chance for pterodactyl
            height = random.choice(PTERODACTYL_HEIGHTS)
            self.obstacles.append(Pterodactyl(height, self.game_speed))

    def update(self):
        if self.game_over:
            return

        # Update Dino
        self.dino.update()

        # Update obstacles
        for obstacle in self.obstacles[:]:
            obstacle.update()
            if obstacle.x < -100:
                self.obstacles.remove(obstacle)
            # Collision detection
            if self.dino.rect.colliderect(obstacle.rect):
                    self.game_over = True

        # Update clouds
        for cloud in self.clouds:
            cloud.update()

        # Score and speed
        self.score += 1
        self.game_speed = min(MAX_SPEED, 8.0 + (self.score * SPEED_INCREMENT))

        # Day/Night transition
        if self.score >= DAY_NIGHT_START:
            self.day_night_factor = min(1.0, (self.score - DAY_NIGHT_START) / (DAY_NIGHT_END - DAY_NIGHT_START))
        else:
            self.day_night_factor = 0.0

        # Spawn new obstacle
        if len(self.obstacles) == 0 or self.obstacles[-1].x < SCREEN_WIDTH - random.randint(OBSTACLE_MIN_GAP, OBSTACLE_MAX_GAP):
            self.spawn_obstacle()

    def draw(self):
        # Sky gradient
        sky_color = [
            int(SKY_DAY[i] + (SKY_NIGHT[i] - SKY_DAY[i]) * self.day_night_factor)
            for i in range(3)
        ]
        self.screen.fill(sky_color)

        # Draw clouds
        for cloud in self.clouds:
            cloud.draw(self.screen)

        # Ground
        pygame.draw.rect(self.screen, GROUND_COLOR, (0, SCREEN_HEIGHT - GROUND_HEIGHT, SCREEN_WIDTH, GROUND_HEIGHT))
        pygame.draw.rect(self.screen, GRASS_COLOR, (0, SCREEN_HEIGHT - GROUND_HEIGHT - GRASS_HEIGHT, SCREEN_WIDTH, GRASS_HEIGHT))

        # Draw Dino
        self.dino.draw(self.screen)

        # Draw obstacles
        for obstacle in self.obstacles:
            obstacle.draw(self.screen)

        # Draw score
        score_text = self.font.render(f"Score: {self.score}", True, SCORE_COLOR)
        self.screen.blit(score_text, (10, 10))

        # Draw high score
        high_score_text = self.font.render(f"High Score: {self.high_score}", True, HIGH_SCORE_COLOR)
        self.screen.blit(high_score_text, (10, 50))

        # Game over screen
        if self.game_over:
            game_over_text = self.font.render("GAME OVER", True, GAME_OVER_COLOR)
            restart_text = self.font.render("Press SPACE to Restart", True, SCORE_COLOR)
            self.screen.blit(game_over_text, (SCREEN_WIDTH // 2 - 80, SCREEN_HEIGHT // 2 - 30))
            self.screen.blit(restart_text, (SCREEN_WIDTH // 2 - 120, SCREEN_HEIGHT // 2 + 10))

        pygame.display.flip()

    def handle_events(self):
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                exit()
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_SPACE:
                    if self.game_over:
                        self.reset()
                    else:
                        self.dino.jump()
                elif event.key == pygame.K_DOWN:
                    self.dino.duck()
                elif event.key == pygame.K_UP:
                    self.dino.jump()
            if event.type == pygame.KEYUP:
                if event.key == pygame.K_DOWN:
                    self.dino.unduck()

    def run(self):
        while True:
            self.handle_events()
            self.update()
            self.draw()
            self.clock.tick(FPS)

            # Update high score
            if self.score > self.high_score:
                self.high_score = self.score
                save_high_score(self.high_score)

if __name__ == "__main__":
    game = Game()
    game.run()