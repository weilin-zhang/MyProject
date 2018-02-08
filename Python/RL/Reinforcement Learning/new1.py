import gym

env = gym.make("CartPole-v0")
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