import pygame
import random
import sys
import os

# Initialize Pygame
pygame.init()

# Constants
SCREEN_WIDTH = 1000
SCREEN_HEIGHT = 400
GROUND_Y = 300
GRAVITY = 0.8
JUMP_STRENGTH = -15
BASE_SPEED = 8.0
DINO_WIDTH = 40
DINO_HEIGHT_STANDING = 60
DINO_HEIGHT_DUCKING = 30
DINO_X = 80
CLOUD_SPAWN_RATE = 0.02  # 2% per frame
OBSTACLE_SPAWN_RATE = 0.015  # 1.5% per frame
MIN_OBSTACLE_GAP = 200  # pixels

# Colors
SKY_DAY = (135, 206, 235)
SKY_NIGHT = (44, 22, 84)
GROUND_BROWN = (139, 69, 19)
GRASS_GREEN = (34, 139, 34)
DINO_BODY = (46, 204, 113)
DINO_LEGS = (39, 174, 96)
EYE_COLOR = (255, 255, 255)
PUPIL_COLOR = (0, 0, 0)
DUCK_COLOR = (128, 128, 128)
CACTUS_COLOR = (26, 92, 26)
PTERODACTYL_COLOR = (230, 126, 34)
CLOUD_COLOR = (255, 255, 255)
TEXT_COLOR = (0, 0, 0)
HIGH_SCORE_COLOR = (255, 215, 0)
GAME_OVER_COLOR = (255, 0, 0)

# Font
FONT = pygame.font.SysFont('Arial', 30, bold=True)
FONT_SMALL = pygame.font.SysFont('Arial', 24)


class Dino:
    def __init__(self):
        self.x = DINO_X
        self.y = GROUND_Y - DINO_HEIGHT_STANDING
        self.width = DINO_WIDTH
        self.height = DINO_HEIGHT_STANDING
        self.rect = pygame.Rect(self.x, self.y, self.width, self.height)
        self.velocity_y = 0
        self.is_jumping = False
        self.is_ducking = False
        self.jump_count = 0  # 0 = grounded, 1 = first jump, 2 = double jump

    def update(self):
        if self.is_jumping:
            self.velocity_y += GRAVITY
            self.y += self.velocity_y
            if self.y >= GROUND_Y - self.height:
                self.y = GROUND_Y - self.height
                self.is_jumping = False
                self.jump_count = 0
        else:
            self.y = GROUND_Y - self.height

        if self.is_ducking:
            self.height = DINO_HEIGHT_DUCKING
        else:
            self.height = DINO_HEIGHT_STANDING

        self.rect.height = self.height
        self.rect.y = self.y

    def jump(self):
        if self.jump_count < 2:
            self.velocity_y = JUMP_STRENGTH
            self.is_jumping = True
            self.jump_count += 1

    def duck(self):
        if not self.is_jumping:
            self.is_ducking = not self.is_ducking

    def draw(self, screen):
        pygame.draw.rect(screen, DINO_BODY, self.rect)
        leg_width = 8
        leg_height = 20
        if self.is_ducking:
            pygame.draw.rect(screen, DINO_LEGS, (self.x + 5, GROUND_Y - 5, leg_width, leg_height))
            pygame.draw.rect(screen, DINO_LEGS, (self.x + self.width - leg_width - 5, GROUND_Y - 5, leg_width, leg_height))
        else:
            pygame.draw.rect(screen, DINO_LEGS, (self.x + 5, self.y + self.height - leg_height, leg_width, leg_height))
            pygame.draw.rect(screen, DINO_LEGS, (self.x + self.width - leg_width - 5, self.y + self.height - leg_height, leg_width, leg_height))
        eye_x = self.x + self.width - 12
        eye_y = self.y + 10
        pygame.draw.circle(screen, EYE_COLOR, (eye_x, eye_y), 5)
        pygame.draw.circle(screen, PUPIL_COLOR, (eye_x + 2, eye_y), 2)
        if self.is_ducking:
            pygame.draw.rect(screen, DUCK_COLOR, (self.x, self.y, self.width, 20))


class Obstacle:
    def __init__(self, x, y, width, height, speed):
        self.rect = pygame.Rect(x, y, width, height)
        self.speed = speed
        self.passed = False

    def update(self):
        self.rect.x -= self.speed
        if self.rect.right < DINO_X and not self.passed:
            self.passed = True

    def draw(self, screen):
        pass

    def collides_with(self, dino):
        return self.rect.colliderect(dino.rect)


class Cactus(Obstacle):
    def __init__(self, x, speed):
        height = random.randint(30, 100)
        trunk_width = 12
        super().__init__(x, GROUND_Y - height, trunk_width, height, speed)
        self.height = height
        self.arms = random.randint(1, 3)
        self.arm_positions = []
        for _ in range(self.arms):
            arm_y = random.randint(GROUND_Y - height + 5, GROUND_Y - 10)
            arm_length = random.randint(15, 25)
            self.arm_positions.append((arm_y, arm_length))

    def draw(self, screen):
        pygame.draw.rect(screen, CACTUS_COLOR, self.rect)
        for arm_y, arm_length in self.arm_positions:
            arm_x = self.rect.x + self.rect.width
            pygame.draw.line(screen, CACTUS_COLOR, (arm_x, arm_y), (arm_x + arm_length, arm_y), 4)


class Pterodactyl(Obstacle):
    def __init__(self, x, speed):
        height_levels = [200, 250, 300]
        y = random.choice(height_levels)
        width = 30
        height = 15
        super().__init__(x, y, width, height, speed)
        self.flap_frame = 0
        self.flap_timer = 0

    def update(self):
        super().update()
        self.flap_timer += 1
        if self.flap_timer >= 10:
            self.flap_frame = 1 - self.flap_frame
            self.flap_timer = 0

    def draw(self, screen):
        pygame.draw.ellipse(screen, PTERODACTYL_COLOR, self.rect)
        x, y, w, h = self.rect.x, self.rect.y, self.rect.width, self.rect.height
        if self.flap_frame == 0:
            wing1 = [(x + 5, y + 5), (x + 15, y - 5), (x + 25, y + 5)]
            wing2 = [(x + 5, y + 10), (x + 15, y), (x + 25, y + 10)]
        else:
            wing1 = [(x + 5, y + 15), (x + 15, y + 25), (x + 25, y + 15)]
            wing2 = [(x + 5, y + 10), (x + 15, y + 20), (x + 25, y + 10)]
        pygame.draw.polygon(screen, PTERODACTYL_COLOR, wing1)
        pygame.draw.polygon(screen, PTERODACTYL_COLOR, wing2)


class Cloud:
    def __init__(self, x, y):
        self.x = x
        self.y = y
        self.width = 60
        self.height = 30
        self.speed = 0.2

    def update(self):
        self.x -= self.speed
        if self.x + self.width < 0:
            self.x = SCREEN_WIDTH + random.randint(50, 200)
            self.y = random.randint(20, 100)

    def draw(self, screen):
        pygame.draw.circle(screen, CLOUD_COLOR, (int(self.x), int(self.y)), 12)
        pygame.draw.circle(screen, CLOUD_COLOR, (int(self.x + 15), int(self.y - 5)), 12)
        pygame.draw.circle(screen, CLOUD_COLOR, (int(self.x + 30), int(self.y)), 12)


class Game:
    def __init__(self):
        flags = 0
        if "DISPLAY" not in os.environ:
            flags = pygame.NOFRAME
        self.screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT), flags)
        pygame.display.set_caption("Chrome Dino Runner")
        self.clock = pygame.time.Clock()
        self.dino = Dino()
        self.obstacles = []
        self.clouds = []
        self.score = 0
        self.high_score = 0
        self.game_speed = BASE_SPEED
        self.day_mode = True
        self.game_over = False
        for _ in range(5):
            x = random.randint(0, SCREEN_WIDTH)
            y = random.randint(20, 100)
            self.clouds.append(Cloud(x, y))

    def run(self):
        running = True
        while running:
            dt = self.clock.tick(60) / 1000.0

            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False
                if event.type == pygame.KEYDOWN:
                    if event.key in (pygame.K_SPACE, pygame.K_UP) and not self.game_over:
                        self.dino.jump()
                    elif event.key == pygame.K_DOWN and not self.game_over:
                        self.dino.duck()
                    elif event.key == pygame.K_SPACE and self.game_over:
                        self.reset()

            if not self.game_over:
                self.dino.update()
                self.update_game_speed()
                self.spawn_obstacle()
                for obstacle in self.obstacles:
                    obstacle.update()
                for cloud in self.clouds:
                    cloud.update()
                self.check_collisions()

            self.draw()

        pygame.quit()
        sys.exit()

    def spawn_obstacle(self):
        if random.random() < OBSTACLE_SPAWN_RATE:
            if len(self.obstacles) == 0 or self.obstacles[-1].rect.x < SCREEN_WIDTH - MIN_OBSTACLE_GAP:
                if random.random() < 0.7:
                    obstacle = Cactus(SCREEN_WIDTH, self.game_speed)
                else:
                    obstacle = Pterodactyl(SCREEN_WIDTH, self.game_speed)
                self.obstacles.append(obstacle)

        if random.random() < CLOUD_SPAWN_RATE:
            x = SCREEN_WIDTH
            y = random.randint(20, 100)
            self.clouds.append(Cloud(x, y))

    def update_game_speed(self):
        if self.score % 100 == 0 and self.score > 0:
            self.game_speed += 0.1

    def check_collisions(self):
        for obstacle in self.obstacles:
            if obstacle.collides_with(self.dino):
                if isinstance(obstacle, Cactus) and self.dino.is_ducking:
                    continue
                self.game_over = True
                break

        self.score += 1
        if self.score > self.high_score:
            self.high_score = self.score

        self.day_mode = self.score < 500

    def draw(self):
        if self.day_mode:
            sky_color = SKY_DAY
        else:
            progress = (self.score - 500) / 500.0
            r = int(SKY_DAY[0] - progress * (SKY_DAY[0] - SKY_NIGHT[0]))
            g = int(SKY_DAY[1] - progress * (SKY_DAY[1] - SKY_NIGHT[1]))
            b = int(SKY_DAY[2] - progress * (SKY_DAY[2] - SKY_NIGHT[2]))
            sky_color = (r, g, b)
        
        self.screen.fill(sky_color)

        for cloud in self.clouds:
            cloud.draw(self.screen)

        pygame.draw.rect(self.screen, GROUND_BROWN, (0, GROUND_Y, SCREEN_WIDTH, 20))
        pygame.draw.rect(self.screen, GRASS_GREEN, (0, GROUND_Y - 5, SCREEN_WIDTH, 5))

        for obstacle in self.obstacles:
            obstacle.draw(self.screen)

        self.dino.draw(self.screen)

        self.draw_ui()

        if self.game_over:
            self.draw_game_over()

        pygame.display.flip()

    def draw_ui(self):
        score_text = FONT.render(f"Score: {self.score}", True, TEXT_COLOR)
        self.screen.blit(score_text, (SCREEN_WIDTH - 150, 20))
        
        high_score_text = FONT.render(f"High: {self.high_score}", True, HIGH_SCORE_COLOR)
        self.screen.blit(high_score_text, (SCREEN_WIDTH - 150, 60))

    def draw_game_over(self):
        game_over_text = FONT.render("GAME OVER", True, GAME_OVER_COLOR)
        restart_text = FONT_SMALL.render("PRESS SPACE TO RESTART", True, TEXT_COLOR)
        
        game_over_rect = game_over_text.get_rect(center=(SCREEN_WIDTH // 2, SCREEN_HEIGHT // 2))
        restart_rect = restart_text.get_rect(center=(SCREEN_WIDTH // 2, SCREEN_HEIGHT // 2 + 40))
        
        self.screen.blit(game_over_text, game_over_rect)
        self.screen.blit(restart_text, restart_rect)

    def reset(self):
        self.dino = Dino()
        self.obstacles = []
        self.score = 0
        self.game_speed = BASE_SPEED
        self.game_over = False


if __name__ == "__main__":
    game = Game()
    game.run()