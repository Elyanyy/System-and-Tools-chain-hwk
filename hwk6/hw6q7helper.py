
import torch
from torch import nn

class myBaseModel(nn.Module):
    def __init__(self,input_dim,output_dim):
        super().__init__()
        self.sequential = nn.Sequential(  # here we stack multiple layers together
            nn.Linear(input_dim,20),
            nn.Tanh(), # Using Tanh activation!
            nn.Linear(20,20),
            nn.Tanh(),
            nn.Linear(20,20),
            nn.Tanh(),
            nn.Linear(20,20),
            nn.Tanh(),
            nn.Linear(20,output_dim)
        )
    def forward(self,x):
        y = self.sequential(x)
        return y

def get_base_model():
    base_model = myBaseModel(113,2)
    base_model.load_state_dict(torch.load("hw6q7_basemodel"))
    return base_model