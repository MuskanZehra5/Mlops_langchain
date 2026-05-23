import pygame
import random
import sys

# Constants
SCREEN_WIDTH = 1200
SCREEN_HEIGHT = 600
FPS = 60
GROUND_HEIGHT = 100
GRASS_HEIGHT = 10
BASE_SPEED = 8
MAX_SPEED = 20
G = 1200  # Gravity px/s²
JUMP_VEL = -450  # px/s
DINO_WIDTH = 60
DINO_HEIGHT_STAND = 70
DINO_HEIGHT_DUCK = 40
DINO_Y = SCREEN_HEIGHT - GROUND_HEIGHT - DINO_HEIGHT_STAND
DINO_DUCK_OFFSET = 30
CLOUD_SPAWN_RATE = 100  # frames
OBSTACLE_MIN_GAP = 200  # px
PTERODACTYL_HEIGHTS = [120, 180, 240]
PTERODACTYL_FLAP_RATE = 15  # frames per flap

# Colors
SKY_DAY = (135, 206, 235)
SKY_NIGHT = (44, 22, 84)
GROUND_COLOR = (139, 69, 19)
GRASS_COLOR = (34, 139, 34)
DINO_BODY = (46, 204, 113)
DINO_LEGS = (39, 174, 96)
EYE_WHITE = (255, 255, 255)
EYE_PUPIL = (0, 0, 0)
CACTUS_COLOR = (26, 92, 26)
PTERODACTYL_COLOR = (230, 126, 34)
CLOUD_COLOR = (255, 255, 255)
SCORE_COLOR = (0, 0, 0)
HIGH_SCORE_COLOR = (255, 215, 0)
GAME_OVER_COLOR = (255, 0, 0)
DUCK_SHAPE = (128, 128, 128)

class Dino:
    def __init__(self):
        self.x = 80
        self.y = DINO_Y
        self.vel_y = 0
        self.on_ground = True
        self.double_jump_used = False
        self.ducking = False
        self.leg_offset = 0
        self.animation_frame = 0

    def update(self, dt, keys):
        if keys[pygame.K_DOWN]:
            self.ducking = True
        else:
            self.ducking = False

        if (keys[pygame.K_SPACE] or keys[pygame.K_UP]) and (self.on_ground or not self.double_jump_used):
            if self.on_ground:
                self.vel_y = JUMP_VEL
                self.on_ground = False
                self.double_jump_used = False
            elif not self.double_jump_used:
                self.vel_y = JUMP_VEL
                self.double_jump_used = True

        if not self.on_ground:
            self.vel_y += G * dt
            self.y += self.vel_y

        if self.y >= DINO_Y:
            self.y = DINO_Y
            self.vel_y = 0
            self.on_ground = True

        self.leg_offset = (self.leg_offset + 1) % 20
        self.animation_frame = (self.animation_frame + 1) % 30

    def get_rect(self):
        if self.ducking:
            return pygame.Rect(self.x, self.y + DINO_DUCK_OFFSET, DINO_WIDTH, DINO_HEIGHT_DUCK)
        else:
            return pygame.Rect(self.x, self.y, DINO_WIDTH, DINO_HEIGHT_STAND)

    def draw(self, screen):
        # Body
        if self.ducking:
            body_rect = pygame.Rect(self.x, self.y + DINO_DUCK_OFFSET, DINO_WIDTH, DINO_HEIGHT_DUCK)
        else:
            body_rect = pygame.Rect(self.x, self.y, DINO_WIDTH, DINO_HEIGHT_STAND)
        
        pygame.draw.rect(screen, DINO_BODY, body_rect, border_radius=8)

        # Legs (animated)
        leg_x = self.x + 10
        leg_y = self.y + (DINO_HEIGHT_STAND if not self.ducking else DINO_HEIGHT_DUCK)
        leg_offset = 5 if self.leg_offset < 10 else -5
        leg_height = 20

        # Left leg
        pygame.draw.rect(screen, DINO_LEGS, pygame.Rect(leg_x, leg_y, 10, leg_height))
        # Right leg (offset)
        pygame.draw.rect(screen, DINO_LEGS, pygame.Rect(leg_x + 40, leg_y + leg_offset, 10, leg_height))

        # Eyes
        eye_x = self.x + 45
        eye_y = self.y + 15
        if self.ducking:
            eye_y += DINO_DUCK_OFFSET  # Adjust eye position when ducking

        # Eye white
        pygame.draw.circle(screen, EYE_WHITE, (eye_x, eye_y), 6)
        # Pupil
        pygame.draw.circle(screen, EYE_PUPIL, (eye_x + 2, eye_y), 2)

class Obstacle:
    def __init__(self, x, y, width, height):
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.speed = BASE_SPEED
        self.passed = False

    def update(self, dt):
        self.x -= self.speed

    def draw(self, screen):
        pass

    def get_rect(self):
        return pygame.Rect(self.x, self.y, self.width, self.height)

class Cactus(Obstacle):
    def __init__(self, x):
        height = random.choice([40, 60, 80])
        super().__init__(x, SCREEN_HEIGHT - GROUND_HEIGHT - height, 20, height)

    def draw(self, screen):
        pygame.draw.rect(screen, CACTUS_COLOR, self.get_rect(), border_radius=5)

class Pterodactyl(Obstacle):
    def __init__(self, x):
        height = random.choice(PTERODACTYL_HEIGHTS)
        super().__init__(x, height, 40, 20)
        self.flap_frame = 0

    def update(self, dt):
        super().update(dt)
        self.flap_frame = (self.flap_frame + 1) % PTERODACTYL_FLAP_RATE

    def draw(self, screen):
        color = PTERODACTYL_COLOR
        rect = self.get_rect()
        if self.flap_frame < PTERODACTYL_FLAP_RATE // 2:
            # Wings up
            pygame.draw.polygon(screen, color, [
                (rect.x + 10, rect.y + 10),
                (rect.x + 30, rect.y),
                (rect.x + 35, rect.y + 10),
                (rect.x + 30, rect.y + 20),
                (rect.x + 10, rect.y + 20)
            ])
        else:
            # Wings down
            pygame.draw.polygon(screen, color, [
                (rect.x + 10, rect.y + 10),
                (rect.x + 30, rect.y + 20),
                (rect.x + 35, rect.y + 10),
                (rect.x + 30, rect.y),
                (rect.x + 10, rect.y + 20)
            ])

class Cloud:
    def __init__(self):
        self.x = SCREEN_WIDTH + random.randint(50, 200)
        self.y = random.randint(50, 200)
        self.speed = 1.5

    def update(self, dt):
        self.x -= self.speed

    def draw(self, screen):
        pygame.draw.circle(screen, CLOUD_COLOR, (int(self.x), int(self.y)), 15)
        pygame.draw.circle(screen, CLOUD_COLOR, (int(self.x) + 10, int(self.y)), 12)
        pygame.draw.circle(screen, CLOUD_COLOR, (int(self.x) + 20, int(self.y)), 10)

class Game:
    def __init__(self):
        pygame.init()
        try:
            self.screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT))
        except:
            self.screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT), pygame.NOFRAME)
        pygame.display.set_caption("Chrome Dino Runner")
        self.clock = pygame.time.Clock()
        self.reset()

    def reset(self):
        self.dino = Dino()
        self.obstacles = []
        self.clouds = []
        self.score = 0
        self.high_score = 0
        self.speed = BASE_SPEED
        self.game_over = False
        self.day_cycle = 0.0
        self.obstacle_timer = 0
        self.cloud_timer = 0

    def handle_events(self):
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                pygame.quit()
                sys.exit()
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_SPACE or event.key == pygame.K_UP:
                    if self.game_over:
                        self.reset()
                if event.key == pygame.K_r and self.game_over:
                    self.reset()

    def update(self, dt):
        if self.game_over:
            return

        # Update Dino
        keys = pygame.key.get_pressed()
        self.dino.update(dt, keys)

        # Spawn clouds
        self.cloud_timer += 1
        if self.cloud_timer >= CLOUD_SPAWN_RATE:
            self.clouds.append(Cloud())
            self.cloud_timer = 0

        # Spawn obstacles
        self.obstacle_timer += 1
        if self.obstacle_timer >= OBSTACLE_MIN_GAP:
            if random.random() < 0.7:
                self.obstacles.append(Cactus(SCREEN_WIDTH))
            else:
                self.obstacles.append(Pterodactyl(SCREEN_WIDTH))
            self.obstacle_timer = 0

        # Update clouds and obstacles
        for cloud in self.clouds[:]:
            cloud.update(dt)
            if cloud.x < -50:
                self.clouds.remove(cloud)

        for obstacle in self.obstacles[:]:
            obstacle.update(dt)
            if obstacle.x < -100:
                self.obstacles.remove(obstacle)
                self.score += 1

            # Collision detection
            if self.dino.get_rect().colliderect(obstacle.get_rect()):
                self.game_over = True
                if self.score > self.high_score:
                    self.high_score = int(self.score)

        # Increase speed over time
        self.speed = min(MAX_SPEED, BASE_SPEED + self.score / 100)
        for obstacle in self.obstacles:
            obstacle.speed = self.speed

        # Day/Night cycle (slowly transition)
        self.day_cycle = (self.day_cycle + 0.0005) % 1

        # Update score
        self.score += 1 * dt  # Increment per second

    def render(self):
        # Sky gradient
        r = int(SKY_DAY[0] + (SKY_NIGHT[0] - SKY_DAY[0]) * self.day_cycle)
        g = int(SKY_DAY[1] + (SKY_NIGHT[1] - SKY_DAY[1]) * self.day_cycle)
        b = int(SKY_DAY[2] + (SKY_NIGHT[2] - SKY_DAY[2]) * self.day_cycle)
        self.screen.fill((r, g, b))

        # Ground
        pygame.draw.rect(self.screen, GROUND_COLOR, (0, SCREEN_HEIGHT - GROUND_HEIGHT, SCREEN_WIDTH, GROUND_HEIGHT))
        pygame.draw.rect(self.screen, GRASS_COLOR, (0, SCREEN_HEIGHT - GROUND_HEIGHT - GRASS_HEIGHT, SCREEN_WIDTH, GRASS_HEIGHT))

        # Clouds
        for cloud in self.clouds:
            cloud.draw(self.screen)

        # Obstacles
        for obstacle in self.obstacles:
            obstacle.draw(self.screen)

        # Dino
        self.dino.draw(self.screen)

        # Score
        font = pygame.font.SysFont("Arial", 30)
        score_text = font.render(f"Score: {int(self.score)}", True, SCORE_COLOR)
        high_score_text = font.render(f"High: {int(self.high_score)}", True, HIGH_SCORE_COLOR)
        self.screen.blit(score_text, (SCREEN_WIDTH - 150, 50))
        self.screen.blit(high_score_text, (SCREEN_WIDTH - 150, 80))

        # Game Over
        if self.game_over:
            overlay = pygame.Surface((SCREEN_WIDTH, SCREEN_HEIGHT), pygame.SRCALPHA)
            overlay.fill((0, 0, 0, 128))
            self.screen.blit(overlay, (0, 0))
            game_over_text = font.render("GAME OVER", True, GAME_OVER_COLOR)
            restart_text = font.render("Press SPACE to Restart", True, SCORE_COLOR)
            self.screen.blit(game_over_text, (SCREEN_WIDTH // 2 - 80, SCREEN_HEIGHT // 2 - 30))
            self.screen.blit(restart_text, (SCREEN_WIDTH // 2 - 120, SCREEN_HEIGHT // 2 + 10))

        pygame.display.flip()

    def run(self):
        running = True
        while running:
            dt = self.clock.tick(FPS) / 1000.0  # Delta time in seconds
            self.handle_events()
            self.update(dt)
            self.render()

if __name__ == "__main__":
    game = Game()
    game.run()