import gym
import time

# env = gym.make('MountainCar-v0')
# env.reset()
# for _ in range(1000):
#     env.render()
#     env.step(env.action_space.sample()) # take a random action

env = gym.make('CartPole-v0')
for i_episode in range(20):
    observation = env.reset()  #重置环境状态
    for t in range(100):
        env.render()   #作为图像引擎的角色，将环境中物体以图像显示
        print(observation)
        action = env.action_space.sample()
        observation, reward, done, info = env.step(action)  #输入动作，输出下一步状态，立即回报，是否终止，调试项
        if done:
            print("Episode finished after {} timesteps".format(t+1))
            break

# env = gym.make('CartPole-v0')   #创造环境
# observation = env.reset()       #初始化环境，observation为环境状态
# count = 0
# for t in range(100):
#     action = env.action_space.sample()  #随机采样动作
#     observation, reward, done, info = env.step(action)  #与环境交互，获得下一步的时刻
#     if done:
#         break
#     env.render()         #绘制场景
#     count+=1
#     time.sleep(0.2)      #每次等待0.2s
# print(count)             #打印该次尝试的步数