﻿using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using PHRLockerAPI.Dto;
using PHRLockerAPI.JwtFeatures;
using System.IdentityModel.Tokens.Jwt;
using Microsoft.AspNetCore.Authentication.JwtBearer;

namespace PHRLockerAPI.Controllers
{
   
    public class LoginController : Controller
    {
        private readonly UserManager<User> _userManager;
        //private readonly IMapper _mapper;
        private readonly JwtHandler _jwtHandler;

        public LoginController(UserManager<User> userManager,JwtHandler jwtHandler)
        {
            _userManager = userManager;
            _jwtHandler = jwtHandler;
        }

        //[HttpPost("Login")] 
        //public async Task<IActionResult> Login([FromBody] UserForAuthenticationDto userForAuthentication)
        //{
        //    var user = await _userManager.FindByNameAsync(userForAuthentication.Email);
        //    if (user == null || !await _userManager.CheckPasswordAsync(user, userForAuthentication.Password))
        //        return Unauthorized(new AuthResponseDto 
        //        { ErrorMessage = "Invalid Authentication" });
        //    var signingCredentials = _jwtHandler.GetSigningCredentials();
        //    var claims = _jwtHandler.GetClaims();
        //    var tokenOptions = _jwtHandler.GenerateTokenOptions(signingCredentials, claims);
        //    var token = new JwtSecurityTokenHandler().WriteToken(tokenOptions);
        //    return Ok(new AuthResponseDto { IsAuthSuccessful = true, Token = token });
        //}
    }
}
